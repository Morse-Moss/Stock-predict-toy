#!/usr/bin/python
# coding=utf-8
from flask import jsonify, Blueprint
import pandas as pd
import numpy as np
import json
from service.stock_spider import EastmoneySpider
from service import tech_util
import service.analysis_util as analysis_util
import random
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, LSTM, LSTM
from sklearn.metrics import mean_absolute_error
from keras.layers import Bidirectional
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error

api_blueprint = Blueprint('api', __name__)

em_spider = EastmoneySpider()


@api_blueprint.route('/search_stock_index/<stock_input>')
def search_stock_index(stock_input):
    """
    搜索大盘指数或个股的行情数据
    """
    market_type = None
    if stock_input == '上证指数':
        stock = {'code': '000001', 'name': '上证指数'}
        market_type = 1
    elif stock_input == '深证成指':
        stock = {'code': '399001', 'name': '深证成指'}
    elif stock_input == '中小板指':
        stock = {'code': '399005', 'name': '中小板指'}
    elif stock_input == '创业板指':
        stock = {'code': '399006', 'name': '创业板指'}
    elif stock_input == '沪深300':
        stock = {'code': '399300', 'name': '沪深300'}
    elif stock_input == '北证50':
        stock = {'code': '899050', 'name': '北证50'}
    else:
        stock = em_spider.stock_index_search(stock_input)

    # 获取该股票的历史数据，前端绘制 K 线图
    # 获取历史K线数据
    stock_df = em_spider.get_stock_kline_factor_datas(security_code=stock['code'], period='day', market_type=market_type)
    stock_df = stock_df[['date', 'open', 'close', 'low', 'high', 'volume']]

    stock_df.sort_values(by='date', ascending=True, inplace=True)
    kline_data = stock_df.values.tolist()

    # 计算 MA 指标
    stock_df['MA5'] = tech_util.MA(stock_df['close'], N=5)
    stock_df['MA10'] = tech_util.MA(stock_df['close'], N=10)
    stock_df['MA20'] = tech_util.MA(stock_df['close'], N=20)
    stock_df['MA60'] = tech_util.MA(stock_df['close'], N=60)

    stock_df.fillna('-', inplace=True)

    return jsonify({
        'name': '{}({})'.format(stock['name'], stock['code']),
        'dates': stock_df['date'].values.tolist(),
        'klines': kline_data,
        'volumes': stock_df['volume'].values.tolist(),
        'tech_datas': {
            'MA5': stock_df['MA5'].values.tolist(),
            'MA10': stock_df['MA10'].values.tolist(),
            'MA20': stock_df['MA20'].values.tolist(),
            'MA60': stock_df['MA60'].values.tolist()
        }
    })


@api_blueprint.route('/query_jibenmian_info/<stock_input>')
def query_jibenmian_info(stock_input):
    """获取基本面信息"""
    stock = em_spider.stock_index_search(stock_input)
    zyzb_table, jgyc_table, gsjj, gsmc = em_spider.get_ji_ben_mian_info(stock['code'])
    # 股票的核心题材
    concept_boards = em_spider.get_stock_core_concepts(stock['code'])
    print(concept_boards)
    
    # 概念板块html
    concept_html = """
    <div class="">
        <div class="card-header">
            <h3>核心概念板块</h3><hr/>
        </div>
        <div class="">
            <table class="table table-hover" style="table-layout:fixed;word-break:break-all;">
            <thead>
                <tr>
                <th scope="col" width="8%">#</th>
                <th scope="col" width="10%">概念板块</th>
                <th scope="col" width="70%">概念解读</th>
                <th scope="col" width="12%">最新涨幅</th>
                </tr>
            </thead>
            <tbody>
                {}
            </tbody>
            </table>
        </div>
        </div>
    """
    trs = ''
    for i, conenpt in enumerate(concept_boards):
        trs += """
        <tr>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td style="color: {}">{}%</td>
        </tr>
        """.format(i+1, conenpt['board_name'], conenpt['board_reason'], 'red' if conenpt['board_yield']>0 else 'green' ,conenpt['board_yield'])
    concept_html = concept_html.format(trs)
        
    return jsonify({
        'zyzb_table': zyzb_table,
        'jgyc_table': jgyc_table,
        'gsjj': gsjj,
        'gsmc': gsmc,
        'concept_boards': concept_html
    })
    

@api_blueprint.route('/limitup_analysis/<trade_date>')
def limitup_analysis(trade_date):
    """涨停板热点分析"""
    print(trade_date)
    trade_date = trade_date.replace('-', '')
    limit_up_stocks = em_spider.get_limit_up_stocks(trade_date=trade_date)
    print(json.dumps(limit_up_stocks, ensure_ascii=False))
    
    trs = ''
    concept_counts = {}
    concept_moneys = {}
    for i, stock in enumerate(limit_up_stocks):
        if stock['行业板块'] not in concept_counts:
            concept_counts[stock['行业板块']] = 0
        concept_counts[stock['行业板块']] += 1
        
        if stock['行业板块'] not in concept_moneys:
            concept_moneys[stock['行业板块']] = 0
        concept_moneys[stock['行业板块']] += stock['成交额']
        
        tr = """
        <tr>
            <th scope="row">{}</th>
            <td><a href="http://127.0.0.1:5000/stock_info?search={}" target="_blank">{}</a></td>
            <td><a href="http://127.0.0.1:5000/stock_info?search={}" target="_blank">{}</a></td>
            <td style="color:red">{}%</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
        """.format(i+1, stock['证券代码'], stock['证券代码'], stock['证券名称'], stock['证券名称'],
                   round(stock['涨跌幅'], 2), stock['最新价'], round(stock['成交额'] / 100000000, 3),
                   round(stock['流通市值'] / 100000000, 3), round(stock['换手率'], 2),
                   round(stock['封板资金'] / 100000000, 3), stock['炸板次数'], 
                   stock['涨停统计'], stock['行业板块'], stock['交易日期'])
        trs += tr
    
    # 行业板块数量分布
    concept_counts = sorted(concept_counts.items(), key=lambda x: x[1], reverse=True)
    print(concept_counts)
    concepts = [c[0] for c in concept_counts]
    # 行业板块资金流入占比

    result = {
        'tbody': trs,
        'concept': concepts,
        'limit_up_count': [c[1] for c in concept_counts],
        'concept_moneys': [concept_moneys[c] for c in concepts]
    }
    return jsonify(result)


@api_blueprint.route('/money_flow_analysis')
def money_flow_analysis():
    """大盘资金流向分析"""
    time_capital_flows = em_spider.get_hu_sheng_two_market_realtime_capital_flow()
    print(time_capital_flows)
    times = []
    # 小单
    xd_capitals = []
    # 中单
    zd_capitals = []
    # 大单
    dd_capitals = []
    # 超大单
    cdd_capitals = []
    # 主力
    zl_capitals = []
    
    for tcf in time_capital_flows:
        times.append(tcf['时间'].split(' ')[1])
        xd_capitals.append(tcf['小单净流入'])
        zd_capitals.append(tcf['中单净流入'])
        dd_capitals.append(tcf['大单净流入'])
        cdd_capitals.append(tcf['超大单净流入'])
        zl_capitals.append(tcf['主力净流入'])
        
    # 南向资金
    north_to_south_flows = em_spider.get_hugangtong_beixiang_nanxiang_capital_flows()
    
    n2s_times = []
    # 港股通(沪)净买额
    ggth_jme = []
    # 港股通(深)净买额
    ggts_jme = []
    # 南向资金
    nxzj = []
    for n2s in north_to_south_flows:
        n2s_times.append(n2s['时间'])
        ggth_jme.append(n2s['港股通(沪)净买额'])
        ggts_jme.append(n2s['港股通(深)净买额'])
        nxzj.append(n2s['南向资金净买额'])     
    result = {
        "时间": times, 
        "小单净流入": xd_capitals, "中单净流入": zd_capitals,
        "大单净流入": dd_capitals, "超大单净流入": cdd_capitals,
        "主力净流入": zl_capitals,
        "南向_时间": n2s_times,
        "港股通(沪)净买额": ggth_jme,
        "港股通(深)净买额": ggts_jme,
        "南向资金净买额": nxzj
    }
    return jsonify(result)


@api_blueprint.route('/market_eval_analysis/<market_eval_type>')
def market_eval_analysis(market_eval_type):
    """
    市场估值分析
    """
    evals = em_spider.get_market_eval(market_eval_type=market_eval_type)
    
    dapan_ttm = evals['eval_values']
    dates = [t['日期'] for t in dapan_ttm]
    ttms = [t['估值'] for t in dapan_ttm]
    
    result = {
        'dates': dates,
        'ttms': ttms,
        'eval_value_30_percentile': evals['eval_value_30_percentile'],
        'eval_value_50_percentile': evals['eval_value_50_percentile'],
        'eval_value_70_percentile': evals['eval_value_70_percentile'],
        'current_percentile': evals['current_percentile'],
    }
    return jsonify(result)


@api_blueprint.route('/stock_quant_analysis/<stock_input>')
def stock_quant_analysis(stock_input):
    """
    股票收益率量化分析与诊股
    """
    market_type = None
    if stock_input == '上证指数':
        stock = {'code': '000001', 'name': '上证指数'}
        market_type = 1
    elif stock_input == '深证成指':
        stock = {'code': '399001', 'name': '深证成指'}
    elif stock_input == '中小板指':
        stock = {'code': '399005', 'name': '中小板指'}
    elif stock_input == '创业板指':
        stock = {'code': '399006', 'name': '创业板指'}
    elif stock_input == '沪深300':
        stock = {'code': '399300', 'name': '沪深300'}
    elif stock_input == '北证50':
        stock = {'code': '899050', 'name': '北证50'}
    else:
        stock = em_spider.stock_index_search(stock_input)

    print(stock)
    # 获取该股票的历史数据，前端绘制 K 线图
    # 获取历史K线数据
    stock_df = em_spider.get_stock_kline_factor_datas(security_code=stock['code'], period='day', market_type=market_type)
    stock_df = stock_df[['date', 'open', 'close', 'low', 'high']]

    stock_df.sort_values(by='date', ascending=True, inplace=True)
    kline_data = stock_df.values.tolist()

    # 计算 BOLL 指标
    stock_df['boll_mid'] = stock_df['close'].rolling(26).mean()
    close_std = stock_df['close'].rolling(20).std()
    stock_df['boll_top'] = stock_df['boll_mid'] + 2 * close_std
    stock_df['boll_bottom'] = stock_df['boll_mid'] - 2 * close_std

    # 计算日收益率
    stock_df['pct_chg'] = stock_df.close.pct_change()
    # 计算对数收益率
    stock_df['log_ret'] = np.log(stock_df.close / stock_df.close.shift(1))
    # 计算累计收益率
    print('对数收益率进行累计求和,可以计算出所有时间点持有的收益率')
    stock_df['cumulative_rets'] = stock_df.log_ret.cumsum().values
    stock_df.fillna({'cumulative_rets': 0}, inplace=True)

    # 计算年化收益率
    year_ret = analysis_util.calc_annualized_returns(stock_df['cumulative_rets'].values[-1], days=stock_df.shape[0])
    # 计算最大回撤
    reward_days = analysis_util.calc_maximum_drawdown(stock_df['cumulative_rets'].values)

    name = '{}({})'.format(stock['name'], stock['code'])
    hint = '{}，年化收益率：<span style="color: {}">{}</span>， 最大回撤：<span style="color: {}">{}</span>'.format(
        name,
        'red' if year_ret > 0 else 'green',
        '{:.4f}%'.format(year_ret * 100),
        'red' if reward_days > 0 else 'green',
        '{:.4f}%'.format(reward_days * 100)
    )

    stock_df.fillna('-', inplace=True)

    return jsonify({
        'name': hint,
        'kline_data': kline_data,
        'boll_data': {
            'UPPER': stock_df['boll_top'].values.tolist(),
            'LOWER': stock_df['boll_bottom'].values.tolist(),
            'MIDDLE': stock_df['boll_mid'].values.tolist()
        },
        'date': stock_df['date'].values.tolist(),
        '日收益率': stock_df['pct_chg'].values.tolist(),
        '日对数收益率': stock_df['log_ret'].values.tolist(),
        '累计收益率': stock_df['cumulative_rets'].values.tolist(),
    })


@api_blueprint.route('/stock_rank')
def stock_rank():
    cur_date, stock_df = em_spider.fetch_stock_main_fund_proportion_rank()
    print(stock_df)
    stock_df = stock_df.sort_values(by='今日涨跌', ascending=False)
    stock_df = stock_df.head(100)

    table_html = ''
    i = 0
    for j, row in stock_df.iterrows():
        i += 1
        tr = f"""
        <tr>
            <td>{i}</td>
            <td {'style="color:red"' if float(row['今日涨跌']) > 0 else 'style="color:green"'} >{row['今日涨跌']}</td>
            <td><a href="http://127.0.0.1:5000/stock_info?search={row['股票代码']}" target="_blank">{row['股票代码']}</a></td>
            <td><a href="http://127.0.0.1:5000/stock_info?search={row['股票名称']}" target="_blank">{row['股票名称']}</a></td>
            <td>{row['所属版块']}</td>
            <td {'style="color:red"' if float(row['5日涨跌']) > 0 else 'style="color:green"'} >{row['5日涨跌']}</td>
            <td {'style="color:red"' if float(row['5日主力净占比']) > 0 else 'style="color:green"'} >{row['5日主力净占比']}</td>
            <td {'style="color:red"' if float(row['10日主力净占比']) > 0 else 'style="color:green"'} >{row['10日主力净占比']}</td>
            <td {'style="color:red"' if float(row['今日主力净占比']) > 0 else 'style="color:green"'} >{row['今日主力净占比']}</td>
        </tr>
        """
        table_html += tr

    return jsonify(table_html)


@api_blueprint.route('/predict_stock_price/<code>/<look_back>/<test_ratio>/<train_epochs>')
def predict_stock_price(code, look_back, test_ratio, train_epochs):
    """股票价格预测（标准化的BiLSTM版）"""
    from sklearn.preprocessing import MinMaxScaler

    # 获取原始数据
    prices_df = em_spider.get_stock_kline_factor_datas(security_code=code, period='day', market_type=None)
    prices_df = prices_df.sort_values(by='date', ascending=True)

    # === 标准化部分 ===
    scaler = MinMaxScaler(feature_range=(0, 1))
    close_prices = prices_df['close'].values.reshape(-1, 1)
    scaled_prices = scaler.fit_transform(close_prices)

    # 分割数据集（保持原始逻辑）
    test_count = int(float(test_ratio) * len(scaled_prices))
    train = scaled_prices[:-test_count].flatten().tolist()  # 标准化后的训练集
    test = scaled_prices[-test_count:].flatten().tolist()  # 标准化后的测试集


    def create_dataset(prehistory, dataset, look_back):
        dataX, dataY = [], []
        history = prehistory.copy()
        for i in range(len(dataset)):
            x = history[i:(i + look_back)]
            y = dataset[i]
            dataX.append(x)
            dataY.append(y)
            history.append(y)
        return np.array(dataX), np.array(dataY)

    look_back = int(look_back)
    trainX, trainY = create_dataset([train[0]] * look_back, train, look_back)
    testX, testY = create_dataset(train[-look_back:], test, look_back)


    def create_bilstm_model():
        d = 0.2
        model = Sequential()
        model.add(Bidirectional(LSTM(16, return_sequences=False), input_shape=(look_back, 1)))
        model.add(Dropout(d))
        model.add(Dense(1, activation='relu'))
        model.compile(loss='mse', metrics=['mae'])
        return model

    model = create_bilstm_model()
    model.fit(trainX.reshape(-1, look_back, 1), trainY,  # 保持原始维度调整方式
              epochs=int(train_epochs),
              batch_size=4,
              verbose=1)

    # 预测并反标准化
    lstm_predictions = model.predict(testX.reshape(-1, look_back, 1))
    lstm_predictions = scaler.inverse_transform(lstm_predictions).flatten().tolist()  # 反标准化

    testY = scaler.inverse_transform([testY]).flatten().tolist()  # 反标准化真实值
    lstm_error = mean_absolute_error(testY, lstm_predictions)

    # 未来预测部分保持原逻辑（需添加标准化处理）
    test_x = scaled_prices[-look_back:].tolist()  # 使用标准化后的最后look_back个值
    test_x = np.array([test_x])

    future_x = []
    for _ in range(10):
        pred = model.predict(test_x.reshape(1, look_back, 1))[0][0]
        future_x.append(float(pred))
        test_x = np.append(test_x[0][1:], pred).reshape(1, -1)

    # 反标准化未来预测
    future_x = scaler.inverse_transform(np.array(future_x).reshape(-1, 1)).flatten().tolist()

    all_time = prices_df['date'].values.tolist() + [f'未来{i + 1}个交易日' for i in range(10)]
    all_data = prices_df['close'].values.tolist()
    lstm_predictions = scaler.inverse_transform([trainY]).flatten().tolist() + lstm_predictions + future_x

    return jsonify({
        'all_time': all_time,
        'all_data': all_data,
        'add_predict': lstm_predictions,
        'test_count': 10,
        'error': lstm_error
    })




