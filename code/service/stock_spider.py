#!/usr/bin/python
# coding=utf-8
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from service import security_util


EASTMONEY_A_BOARD_FIELDS = {
    'f2': '最新价',
    'f3': '涨跌幅',
    'f4': '涨跌额',
    'f5': '成交量',
    'f6': '成交额',
    'f7': '振幅',
    'f8': '换手率',
    'f9': '动态市盈率',
    'f10': '量比',
    'f12': '证券代码',
    'f14': '证券名称',
    'f15': '最高',
    'f16': '最低',
    'f17': '今开',
    'f18': '昨日收盘',
    'f20': '总市值',
    'f21': '流通市值',
    'f13': '市场编号',
    'f124': '更新时间戳',
    'f297': '最新交易日',
}


class EastmoneySpider(object):
    """
    东方财富网络爬虫
    """

    def __init__(self):
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            "Cookie": "intellpositionL=1152px; IsHaveToNewFavor=0; qgqp_b_id=dbe20efaab3321e948962637a37ac894; em-quote-version=topspeed; em_hq_fls=js; emhq_picfq=2; _qddaz=QD.fqkydg.323wae.klgkordo; st_si=68830202953408; emshistory=%5B%22%E5%8C%97%E5%90%91%E8%B5%84%E9%87%91%22%2C%22%E9%BB%84%E5%8D%8E%E6%9F%92%22%2C%22603501%22%2C%22603501.SH%22%2C%22%E7%AB%8B%E8%AE%AF%E7%B2%BE%E5%AF%86%22%2C%22%E9%87%91%E9%BE%99%E9%B1%BC%22%2C%22000001%22%2C%22%E4%B8%AD%E8%8A%AF%E5%9B%BD%E9%99%85%22%5D; p_origin=https%3A%2F%2Fpassport2.eastmoney.com; testtc=0.5378301696721359; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=06-23 23:41:40@#$%u5357%u534E%u4E30%u6DF3%u6DF7%u5408A@%23%24005296; sid=112627825; vtpst=|; HAList=a-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Ca-sz-300999-%u91D1%u9F99%u9C7C%2Ca-sh-600199-%u91D1%u79CD%u5B50%u9152%2Ca-sh-601279-%u82F1%u5229%u6C7D%u8F66%2Ca-sz-002261-%u62D3%u7EF4%u4FE1%u606F%2Ca-sz-002570-%u8D1D%u56E0%u7F8E%2Ca-sz-000150-%u5B9C%u534E%u5065%u5EB7%2Ca-sz-300785-%u503C%u5F97%u4E70%2Ca-sz-003039-%u987A%u63A7%u53D1%u5C55%2Ca-sz-000158-%u5E38%u5C71%u5317%u660E%2Ca-sz-002044-%u7F8E%u5E74%u5065%u5EB7%2Ca-sz-002475-%u7ACB%u8BAF%u7CBE%u5BC6; cowCookie=true; cowminicookie=true; st_asi=delete; st_pvi=89273277965854; st_sp=2020-07-21%2011%3A22%3A06; st_inirUrl=http%3A%2F%2Fdata.eastmoney.com%2Fbkzj%2FBK0473.html; st_sn=186; st_psi=20210707231408251-111000300841-0970974274; intellpositionT=2215px",
            "Host": None,
            "Referer": None,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }

    def stock_index_search(self, keyword):
        """
        指数或个股的搜索接口
        """
        accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'
        eastmoney_fund_cookie = 'intellpositionL=1152px; IsHaveToNewFavor=0; qgqp_b_id=dbe20efaab3321e948962637a37ac894; cowminicookie=true; emshistory=%5B%22000001%22%2C%22%E4%B8%AD%E8%8A%AF%E5%9B%BD%E9%99%85%22%5D; em-quote-version=topspeed; st_si=10129111187284; intellpositionT=455px; st_asi=delete; em_hq_fls=js; emhq_picfq=2; EMFUND0=02-14%2012%3A54%3A48@%23%24%u6613%u65B9%u8FBE%u4E2D%u8BC1%u56FD%u4F01%u5E26%u8DEF%u53D1%u8D77%u5F0F%u8054%u63A5C@%23%24007789; EMFUND1=02-14%2012%3A55%3A09@%23%24%u9E4F%u534E%u4E2D%u8BC1500ETF%u8054%u63A5C@%23%24008001; EMFUND2=02-14%2012%3A55%3A36@%23%24%u5E73%u5B89500ETF%u8054%u63A5C@%23%24006215; EMFUND3=02-14%2013%3A48%3A04@%23%24%u534E%u5546%u53CC%u64CE%u9886%u822A%u6DF7%u5408@%23%24010550; EMFUND4=02-14%2013%3A51%3A20@%23%24%u534E%u6CF0%u67CF%u745E%u6210%u957F%u667A%u9009%u6DF7%u5408A@%23%24010345; EMFUND5=02-14%2013%3A53%3A04@%23%24%u6613%u65B9%u8FBE%u4E2D%u8BC1800ETF%u8054%u63A5C@%23%24007857; EMFUND6=02-14%2016%3A09%3A53@%23%24%u535A%u65F6%u5065%u5EB7%u6210%u957F%u53CC%u5468%u5B9A%u671F%u53EF%u8D4E%u56DE%u6DF7%u5408A@%23%24009468; EMFUND7=02-14%2016%3A09%3A53@%23%24%u535A%u65F6%u4EA7%u4E1A%u7CBE%u9009%u6DF7%u5408C@%23%24010456; HAList=a-sz-002352-%u987A%u4E30%u63A7%u80A1%2Ca-sh-601012-%u9686%u57FA%u80A1%u4EFD%2Cd-hk-00700%2Ca-sz-300122-%u667A%u98DE%u751F%u7269%2Cd-hk-06886%2Ca-sz-300815-%u7389%u79BE%u7530%2Ca-sz-002475-%u7ACB%u8BAF%u7CBE%u5BC6%2Ca-sh-603655-%u6717%u535A%u79D1%u6280%2Ca-sz-000001-%u5E73%u5B89%u94F6%u884C%2Ca-sh-600093-%u6613%u89C1%u80A1%u4EFD%2Ca-sz-002668-%u5965%u9A6C%u7535%u5668%2Ca-sz-002982-%u6E58%u4F73%u80A1%u4EFD; EMFUND9=02-14%2016%3A37%3A43@%23%24%u4E1C%u5434%u884C%u4E1A%u8F6E%u52A8%u6DF7%u5408C@%23%24011240; EMFUND8=02-14 17:32:32@#$%u6731%u96C0%u4EA7%u4E1A%u81FB%u9009%u6DF7%u5408C@%23%24007494; st_pvi=89273277965854; st_sp=2020-07-21%2011%3A22%3A06; st_inirUrl=http%3A%2F%2Fdata.eastmoney.com%2Fbkzj%2FBK0473.html; st_sn=179; st_psi=20210214173232100-0-7604659170'

        search_api = 'http://searchapi.eastmoney.com/api/suggest/get?cb=jQuery1124031929354940331467_1613296313722&input={}&type=8&token=D43BF722C8E33BDC906FB84D85E326E8&markettype=&mktnum=&jys=&classify=&securitytype=&status=&count=4&_={}'
        url = search_api.format(keyword, int(time.time() * 1000))
        print(url)
        headers = {
            'Accept': accept,
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Cookie': eastmoney_fund_cookie,
            'Host': 'searchapi.eastmoney.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': user_agent
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf8'
        similar_results = json.loads(resp.text.split('({"GubaCodeTable":')[1][:-2])['Data']

        if similar_results:
            return {'code': similar_results[0]['OuterCode'], 'name': similar_results[0]['ShortName']}
        else:
            return None
        
    
    def get_stock_kline_factor_datas(self, security_code, period, market_type):
        """
        获取个股的 K 线和基本指标数据

        Args:
            security_code: 股票代码
            period: 周期: day、week、month
        """
        if not market_type:
            security_type = security_util.get_security_type(security_code)
            market_type = int(security_type == 'SH')
        print('market_type:', market_type)

        # 根据当前时间，计算 beg 值
        cur_date = datetime.now()
        if period == 'day':
            begin_date = cur_date + timedelta(days=-1200)
            begin_date = begin_date.strftime('%Y%m%d')
            url = f'https://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg={begin_date}&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&rtntype=6&secid={market_type}.{security_code}&klt=101&fqt=1'
        elif period == 'week':
            begin_date = cur_date + timedelta(days=-120)
            begin_date = begin_date.strftime('%Y%m%d')
            url = f'https://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg={begin_date}&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&rtntype=6&secid={market_type}.{security_code}&klt=102&fqt=1'
        elif period == 'month':
            begin_date = cur_date + timedelta(days=-250)
            begin_date = begin_date.strftime('%Y%m%d')
            url = f'https://push2his.eastmoney.com/api/qt/stock/kline/get?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg={begin_date}&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&rtntype=6&secid={market_type}.{security_code}&klt=103&fqt=1'
        else:
            raise ValueError(f'暂不支持 {period} 类型周期')

        resp = requests.get(url)
        resp.encoding = 'utf8'
        resp_data = resp.json()['data']
        security_name = resp_data['name']
        klines = resp.json()['data']['klines']

        all_stock_info = []
        for kline in klines:
            # 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
            datas = kline.split(',')
            stock_info = {
                'date': datas[0],
                'code': security_code,
                'name': security_name,
                'open': float(datas[1]),
                'close': float(datas[2]),
                'high': float(datas[3]),
                'low': float(datas[4]),
                'volume': float(datas[6])
            }
            all_stock_info.append(stock_info)

        stock_df = pd.DataFrame(all_stock_info)
        stock_df.to_csv(f"./dataset/kline/{security_code}.csv", index=False, encoding='utf8')
        return stock_df
    
    
    def get_ji_ben_mian_info(self, stock_code):
        """基本面信息获取"""
        # 主要指标
        url = 'http://emweb.securities.eastmoney.com/PC_HSF10/OperationsRequired/OperationsRequiredAjax?times=1&code={}'
        stock_type = security_util.get_security_type(stock_code)
        stock_code = '{}{}'.format(stock_type, stock_code)

        url = url.format(stock_code)
        print(url)
        resp = requests.get(url)
        result = resp.json()
        # 主要指标表格
        zyzb1_table = result['zxzb1'].replace('<table>', '<table class="table table-bordered">')
        zyzb_table = zyzb1_table

        # 机构预测
        jgyc_trs = ''
        for jg in result['jgyc'][:10]:
            tr = '<tr>'
            for v in jg.values():
                tr += '<td class="tips-dataC">' + v + '</td>'
            tr += '</tr>'
            jgyc_trs += tr

        jgyc_table = """
        <table class="table table-bordered" >
        <tbody>
            <tr>
                <th rowspan="2" class="tips-colnameC">机构名称</th>
                <th colspan="2" class="tips-colnameC" width="88">2020A</th>
                <th colspan="2" class="tips-colnameC" width="88">2021E</th>
                <th colspan="2" class="tips-colnameC" width="88">2022E</th>
                <th colspan="2" class="tips-colnameC" width="88">2023E</th>
            </tr>
            <tr>
                <th class="tips-weightnormal tips-dataC">收益</th>
                <th class="tips-weightnormal tips-dataC">市盈率</th>
                <th class="tips-weightnormal tips-dataC">收益</th>
                <th class="tips-weightnormal tips-dataC">市盈率</th>
                <th class="tips-weightnormal tips-dataC">收益</th>
                <th class="tips-weightnormal tips-dataC">市盈率</th>
                <th class="tips-weightnormal tips-dataC">收益</th>
                <th class="tips-weightnormal tips-dataC">市盈率</th>
            </tr>
        """
        jgyc_table += jgyc_trs + '</tbody></table>'

        # 公司简介
        url = 'https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code={}'
        url = url.format(stock_code)
        resp = requests.get(url)
        result = resp.json()
        gsjj = result['jbzl']['gsjj']
        gsmc = result['jbzl']['gsmc']
        return zyzb_table, jgyc_table, gsjj, gsmc


    def get_stock_core_concepts(self, stock_code):
        """
        实时获取股票的核心题材

        https://emweb.securities.eastmoney.com/pc_hsf10/pages/index.html?type=web&code=SZ300006&color=b#/hxtc
        """
        stock_code = security_util.security_code_norm(stock_code)
        base_url = "https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_F10_CORETHEME_BOARDTYPE&columns=SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NEW_BOARD_CODE,BOARD_NAME,SELECTED_BOARD_REASON,IS_PRECISE,BOARD_RANK,BOARD_YIELD,DERIVE_BOARD_CODE&quoteColumns=f3~05~NEW_BOARD_CODE~BOARD_YIELD&" \
                "filter=(SECUCODE%3D%22{}%22)(IS_PRECISE%3D%221%22)&pageNumber=1&pageSize=&sortTypes=1&sortColumns=BOARD_RANK&source=HSF10&client=PC&v=04027857629400182"

        url = base_url.format(stock_code)

        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Connection': 'keep-alive',
            "Cookie": "intellpositionL=1152px; IsHaveToNewFavor=0; qgqp_b_id=dbe20efaab3321e948962637a37ac894; em-quote-version=topspeed; em_hq_fls=js; emhq_picfq=2; _qddaz=QD.fqkydg.323wae.klgkordo; st_si=68830202953408; emshistory=%5B%22%E5%8C%97%E5%90%91%E8%B5%84%E9%87%91%22,%22%E9%BB%84%E5%8D%8E%E6%9F%92%22,%22603501%22,%22603501.SH%22,%22%E7%AB%8B%E8%AE%AF%E7%B2%BE%E5%AF%86%22,%22%E9%87%91%E9%BE%99%E9%B1%BC%22,%22000001%22,%22%E4%B8%AD%E8%8A%AF%E5%9B%BD%E9%99%85%22%5D; p_origin=https%3A%2F%2Fpassport2.eastmoney.com; testtc=0.5378301696721359; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=06-23 23:41:40@#$%u5357%u534E%u4E30%u6DF3%u6DF7%u5408A@%23%24005296; sid=112627825; vtpst=|; HAList=a-sz-300059-%u4E1C%u65B9%u8D22%u5BCC,a-sz-300999-%u91D1%u9F99%u9C7C,a-sh-600199-%u91D1%u79CD%u5B50%u9152,a-sh-601279-%u82F1%u5229%u6C7D%u8F66,a-sz-002261-%u62D3%u7EF4%u4FE1%u606F,a-sz-002570-%u8D1D%u56E0%u7F8E,a-sz-000150-%u5B9C%u534E%u5065%u5EB7,a-sz-300785-%u503C%u5F97%u4E70,a-sz-003039-%u987A%u63A7%u53D1%u5C55,a-sz-000158-%u5E38%u5C71%u5317%u660E,a-sz-002044-%u7F8E%u5E74%u5065%u5EB7,a-sz-002475-%u7ACB%u8BAF%u7CBE%u5BC6; cowCookie=true; cowminicookie=true; st_asi=delete; st_pvi=89273277965854; st_sp=2020-07-21%2011%3A22%3A06; st_inirUrl=http%3A%2F%2Fdata.eastmoney.com%2Fbkzj%2FBK0473.html; st_sn=186; st_psi=20210707231408251-111000300841-0970974274; intellpositionT=2215px",
            "Host": "datacenter.eastmoney.com",
            "Origin": "https://emweb.securities.eastmoney.com",
            "Referer": "https://emweb.securities.eastmoney.com/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }
        try:
            resp = requests.get(url, headers=headers)
            resp.encoding = 'utf8'
            concepts = resp.json()['result']['data']

            concept_boards = []
            for concept in concepts:
                concept_boards.append({
                    "board_name": concept['BOARD_NAME'],
                    "board_code": concept['NEW_BOARD_CODE'],
                    "board_reason": concept['SELECTED_BOARD_REASON'],
                    "board_yield": concept['BOARD_YIELD'],
                    "board_rank": concept['BOARD_RANK']
                })
        except:
            concept_boards = []
        return concept_boards


    def fetch_stock_capital_flow_rank(self, days=0):
        """
        获取东方财富网站的个股资金流的最新排名

        fid 次数代表统计的时间范围，f62：今日排名，f267：3日排名，f164：5日排名，f174：10日排名
        http://data.eastmoney.com/zjlx/detail.html

        Args:
            days: 间隔的时间，0：f62，今日排名；3，f267,3日排名；5，f164，5日排名；10，f174，10日排名
        """
        page = 1
        page_size = 10000

        if days == 0:
            fid = 'f62'
        elif days == 3:
            fid = 'f267'
        elif days == 5:
            fid = 'f164'
        elif days == 10:
            fid = 'f174'
        else:
            raise ValueError('not supported days, Only 0, 3, 5, 10 can be selected')

        url = f'https://push2.eastmoney.com/api/qt/clist/get?fid={fid}&po=1&pz={page_size}&pn={page}&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124%2Cf1%2Cf13'
        self.logger.info(url)

        self.headers['Host'] = "push2.eastmoney.com"
        self.headers['Referer'] = "http://data.eastmoney.com/"

        resp = requests.get(url, headers=self.headers)
        resp.encoding = 'utf8'
        stock_datas = json.loads(resp.text)['data']['diff']

        # 当前统计的日期
        cur_date = stock_datas[0]['f124']
        cur_date = datetime.fromtimestamp(cur_date)
        # 转成 dataframe
        stock_df = pd.DataFrame(stock_datas)

        rename_columns = {
            "f12": "股票代码",
            "f14": "股票名称",
            "f62": f"{days}日主力净流入_净额",
            "f184": f"{days}日主力净流入_净占比",
        }
        stock_df.rename(columns=rename_columns, inplace=True)

        for col in rename_columns.values():
            stock_df = stock_df[stock_df[col] != '-']
            if col not in {'股票代码', '股票名称'}:
                stock_df[col] = stock_df[col].astype(float)

        drop_coumns = [f for f in stock_df.columns.tolist() if f not in set(rename_columns.values())]
        stock_df.drop(drop_coumns, axis=1, inplace=True)
        stock_df[f'{days}日排名'] = range(1, stock_df.shape[0]+1)
        return cur_date, stock_df

    def fetch_stock_main_fund_proportion_rank(self):
        """
        个股主力资金占比排名
        http://data.eastmoney.com/zjlx/list.html
        """
        page = 1
        page_size = 10000
        url = f'http://push2.eastmoney.com/api/qt/clist/get?fid=f184&po=1&pz={page_size}&pn={page}&np=1&fltt=2&invt=2&fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2'

        self.headers['Host'] = "push2.eastmoney.com"
        self.headers['Referer'] = "http://data.eastmoney.com/"

        resp = requests.get(url, headers=self.headers)
        resp.encoding = 'utf8'
        stock_datas = json.loads(resp.text)['data']['diff']
        # 当前统计的日期
        cur_date = stock_datas[0]['f124']
        cur_date = datetime.fromtimestamp(cur_date)

        stock_df = pd.DataFrame(stock_datas)
        rename_columns = {
                "f12": "股票代码",
                "f14": "股票名称",
                "f184": "今日主力净占比",
                "f3": "今日涨跌",
                "f165": "5日主力净占比",
                "f109": "5日涨跌",
                "f175": "10日主力净占比",
                "f160": "10日涨跌",
                "f100": "所属版块",

            }
        stock_df.rename(columns=rename_columns, inplace=True)

        for col in rename_columns.values():
            stock_df = stock_df[stock_df[col] != '-']
            if col not in {'股票代码', '股票名称', '所属版块'}:
                stock_df[col] = stock_df[col].astype(float)

        drop_coumns = [f for f in stock_df.columns.tolist() if f not in set(rename_columns.values())]
        stock_df.drop(drop_coumns, axis=1, inplace=True)
        return cur_date, stock_df

    def fetch_stock_north_bound_foreign_capital_rank(self):
        """
        个股北向资金持仓排名，注意是上一个交易日的数据
        http://data.eastmoney.com/hsgtcg/list.html
        """
        page = 1
        page_size = 10000
        HdDate = datetime.now().date()

        while True:
            url = f'https://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?st=ShareSZ_Chg_One&sr=-1&ps={page_size}&p={page}&type=HSGT20_GGTJ_SUM&token=894050c76af8597a853f5b408b759f5d&js=%7B%22data%22%3A(x)%2C%22pages%22%3A(tp)%2C%22font%22%3A(font)%7D&filter=(DateType%3D%271%27)(HdDate%3D%27{str(HdDate)}%27)'
            self.logger.info(url)

            self.headers['Host'] = "dcfm.eastmoney.com"
            self.headers['Referer'] = "http://data.eastmoney.com/"

            resp = requests.get(url, headers=self.headers)
            resp.encoding = 'utf8'
            stock_datas = json.loads(resp.text)['data']

            if len(stock_datas) > 0:
                break
            HdDate = HdDate + timedelta(days=-1)

        stock_df = pd.DataFrame(stock_datas)
        rename_columns = {
            "SCode": "股票代码",
            "SName": "股票名称",
            "HYName": "所属行业",
            "HYCode": "行业代码",
            "DQName": "所属地区",
            "DQCode": "地区代码",
            "ShareHold": "今日持股股数",
            "ShareSZ": "今日持股市值",
            "LTZB": "今日持股占流通股比",
            "ZZB": "今日持股占总股本比",
            "ShareHold_Chg_One": "今日增持股数",
            "ShareSZ_Chg_One": "今日增持市值",
            "LTZB_One": "今日增持占流通股比‰",
            "ZZB_One": "今日增持占总股本比‰",
        }
        stock_df.rename(columns=rename_columns, inplace=True)
        for col in rename_columns.values():
            stock_df = stock_df[stock_df[col] != '-']
            if col not in {'股票代码', '股票名称', '所属版块', '所属行业', '行业代码', '所属地区', '地区代码'}:
                stock_df[col] = stock_df[col].astype(float)

        drop_coumns = [f for f in stock_df.columns.tolist() if f not in set(rename_columns.values())]
        stock_df.drop(drop_coumns, axis=1, inplace=True)
        return HdDate, stock_df

    def fetch_stock_commodity_rank(self):
        """
        个股大宗交易排名
        http://data.eastmoney.com/dzjy/dzjy_mrtj.html
        """
        page = 1
        page_size = 10000
        trade_date = datetime.now().date()

        while True:
            url = f'http://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=TURNOVERRATE&sortTypes=-1&pageSize={page_size}&pageNumber={page}&reportName=RPT_BLOCKTRADE_STA&columns=TRADE_DATE%2CSECURITY_CODE%2CSECUCODE%2CSECURITY_NAME_ABBR%2CCHANGE_RATE%2CCLOSE_PRICE%2CAVERAGE_PRICE%2CPREMIUM_RATIO%2CDEAL_NUM%2CVOLUME%2CDEAL_AMT%2CTURNOVERRATE%2CD1_CLOSE_ADJCHRATE%2CD5_CLOSE_ADJCHRATE%2CD10_CLOSE_ADJCHRATE%2CD20_CLOSE_ADJCHRATE&source=WEB&client=WEB&filter=(TRADE_DATE%3D%27{str(trade_date)}%27)'
            self.logger.info(url)

            self.headers['Host'] = "datacenter-web.eastmoney.com"
            self.headers['Referer'] = "http://data.eastmoney.com/"
            self.headers['Cookie'] = 'intellpositionL=1152px; IsHaveToNewFavor=0; qgqp_b_id=dbe20efaab3321e948962637a37ac894; em-quote-version=topspeed; em_hq_fls=js; emhq_picfq=2; _qddaz=QD.fqkydg.323wae.klgkordo; st_si=68830202953408; emshistory=%5B%22%E5%8C%97%E5%90%91%E8%B5%84%E9%87%91%22%2C%22%E9%BB%84%E5%8D%8E%E6%9F%92%22%2C%22603501%22%2C%22603501.SH%22%2C%22%E7%AB%8B%E8%AE%AF%E7%B2%BE%E5%AF%86%22%2C%22%E9%87%91%E9%BE%99%E9%B1%BC%22%2C%22000001%22%2C%22%E4%B8%AD%E8%8A%AF%E5%9B%BD%E9%99%85%22%5D; p_origin=https%3A%2F%2Fpassport2.eastmoney.com; testtc=0.5378301696721359; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; EMFUND5=null; EMFUND6=null; EMFUND7=null; EMFUND8=null; EMFUND0=null; EMFUND9=06-23 23:41:40@#$%u5357%u534E%u4E30%u6DF3%u6DF7%u5408A@%23%24005296; sid=112627825; vtpst=|; HAList=a-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Ca-sz-300999-%u91D1%u9F99%u9C7C%2Ca-sh-600199-%u91D1%u79CD%u5B50%u9152%2Ca-sh-601279-%u82F1%u5229%u6C7D%u8F66%2Ca-sz-002261-%u62D3%u7EF4%u4FE1%u606F%2Ca-sz-002570-%u8D1D%u56E0%u7F8E%2Ca-sz-000150-%u5B9C%u534E%u5065%u5EB7%2Ca-sz-300785-%u503C%u5F97%u4E70%2Ca-sz-003039-%u987A%u63A7%u53D1%u5C55%2Ca-sz-000158-%u5E38%u5C71%u5317%u660E%2Ca-sz-002044-%u7F8E%u5E74%u5065%u5EB7%2Ca-sz-002475-%u7ACB%u8BAF%u7CBE%u5BC6; cowCookie=true; cowminicookie=true; ct=G0hDUs9gKQi4aW3xEvD_nUrvLeySSKACcjb7pt3PMuXGFTG6vFrXgU2TgPWTwf0rdVDMadZVeZigKBdt7gEhjYNn-RAz71rx4ymc2WaoxFJ_DrbmougHAvgzabrvCDKIsufTnqqSWBv6Q7YBPwmh9axru9ZquwZx92r6AdmT8Wg; ut=FobyicMgeV6oOlrtxUaVohmqCX7oh_O3yYZj6h8pdH-y_j-3oLUInf8bY9Ltl5f6Ki3pD_dO18HVqwCVuj1QyYJHLPkGETogY_ap7tz0wKJXzFJDtSmVcrzoevDqsYUBPGCv5dW5brKbArK3fBLyWzpQgl5n5MAk_OzmiEqnm51rW36tdorfCNXhVKg5yk-63EQHMLUW9L6Udk014KnVVkrMRaKd8abrVT_Gjm9muJBGNT39TG5KMpoZ62yiZy6FoSafAw4HWQIOqXw-mDGlHNghBD8PfPjD; st_asi=delete; intellpositionT=708px; JSESSIONID=22EE02E35CAF6CCCDE9D3E0D02F7AFFF; st_pvi=89273277965854; st_sp=2020-07-21%2011%3A22%3A06; st_inirUrl=http%3A%2F%2Fdata.eastmoney.com%2Fbkzj%2FBK0473.html; st_sn=222; st_psi=20210708150358412-113300300970-2924897269'

            resp = requests.get(url, headers=self.headers)
            resp.encoding = 'utf8'
            stock_datas = json.loads(resp.text)['result']

            if stock_datas is not None and len(stock_datas) > 0:
                stock_datas = stock_datas['data']
                break
            trade_date = trade_date + timedelta(days=-1)

        stock_df = pd.DataFrame(stock_datas)
        rename_columns = {
            "SECURITY_CODE": "股票代码",
            "SECURITY_NAME_ABBR": "股票名称",
            "CHANGE_RATE": "当日涨跌幅",
            "CLOSE_PRICE": "当日收盘价",
            "AVERAGE_PRICE": "大宗交易均价",
            "PREMIUM_RATIO": "大宗交易折溢率",
            "DEAL_NUM": "大宗交易笔数",
            "VOLUME": "成交总量(万股)",
            "DEAL_AMT": "成交总额(万元)",
            "TURNOVERRATE": "成交总额/流通市值"
        }
        stock_df.rename(columns=rename_columns, inplace=True)
        for col in rename_columns.values():
            stock_df = stock_df[stock_df[col] != '-']
            if col not in {'股票代码', '股票名称'}:
                stock_df[col] = stock_df[col].astype(float)

        drop_coumns = [f for f in stock_df.columns.tolist() if f not in set(rename_columns.values())]
        stock_df.drop(drop_coumns, axis=1, inplace=True)
        return trade_date, stock_df

    def get_limit_up_stocks(self, trade_date, page_index=0, pagesize=100):
        """
        获取交易日的涨停板数据，注意东方财富网站中收录的涨停板不包含 ST 股
        http://quote.eastmoney.com/ztb/detail#type=ztgc

        Args:
            trade_date: 交易日期，%Y%m%d 格式，20230901
            page_index: 当前页下标
            pagesize: 分页大小，默认最大千股涨停。。。
        """
        time_token = int(time.time() * 1000)
        base_url = 'https://push2ex.eastmoney.com/getTopicZTPool?ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wz.ztzt&Pageindex={}&pagesize={}&sort=fbt%3Aasc&date={}&_={}'
        url = base_url.format(page_index, pagesize, trade_date, time_token)
        print(url)
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            "Cookie": "qgqp_b_id=5dd5a3c880eed4135c765cec5bbdf661; websitepoptg_api_time=1722910590758; st_si=11292188228136; HAList=ty-0-300147-%u9999%u96EA%u5236%u836F%2Cty-1-600789-%u9C81%u6297%u533B%u836F%2Cty-1-600095-%u6E58%u8D22%u80A1%u4EFD%2Cty-0-300059-%u4E1C%u65B9%u8D22%u5BCC%2Cty-1-000001-%u4E0A%u8BC1%u6307%u6570%2Cty-0-399300-%u6CAA%u6DF1300; st_asi=delete; st_pvi=88716095105714; st_sp=2024-01-04%2013%3A36%3A25; st_inirUrl=http%3A%2F%2F127.0.0.1%3A8080%2F; st_sn=21; st_psi=20240806114842185-113200304537-8838924605",
            "Host": "push2ex.eastmoney.com",
            "Referer": "https://quote.eastmoney.com/ztb/detail",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf8'
        resp = json.loads(resp.text)['data']
        if resp is None:
            return []

        stock_datas = resp['pool']

        columns_map = {
            'c': '证券代码',
            'n': '证券名称',
            'zdp': '涨跌幅',
            'p': '最新价',
            'amount': '成交额',
            'ltsz': '流通市值',
            'tshare': '流通市值',
            'hs': '换手率',
            'fund': '封板资金',
            'zbc': '炸板次数',
            'zttj': '涨停统计',
            'hybk': '行业板块'
        }

        limit_up_stocks = []
        for stock_info in stock_datas:
            limit_up_stock = {}
            for c in stock_info:
                if c in columns_map:
                    value = stock_info[c]
                    if c == 'zttj':
                        value = '{}天{}板'.format(stock_info[c]['days'], stock_info[c]['ct'])
                    limit_up_stock[columns_map[c]] = value
            limit_up_stock['交易日期'] = trade_date
            limit_up_stocks.append(limit_up_stock)

        return limit_up_stocks


    def get_hu_sheng_two_market_realtime_capital_flow(self):
        """
        沪深两市实时资金流
        """
        time_token = int(time.time() * 1000)
        base_url = "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get?lmt=0&klt=1&fields1=f1%2Cf2%2Cf3%2Cf7&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61%2Cf62%2Cf63%2Cf64%2Cf65&ut=b2884a393a59ad64002292a3e90d46a5&secid=1.000001&_={}"
        url = base_url.format(time_token)
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Connection': 'keep-alive',
            "Cookie": "qgqp_b_id=a9a259420563b8c14556a92ff2af9233; emshistory=%5B%22600852%22%2C%22600849%22%2C%22600840%22%2C%22600530%22%2C%22002799%22%2C%22000890%22%5D; websitepoptg_api_time=1695613205747; st_si=99817584399103; st_asi=delete; HAList=ty-1-000001-%u4E0A%u8BC1%u6307%u6570%2Cty-1-600530-*ST%u4EA4%u6602%2Cty-0-002799-%u73AF%u7403%u5370%u52A1%2Cty-0-000890-%u6CD5%u5C14%u80DC%2Cty-0-000584-ST%u5DE5%u667A; st_pvi=70429278548518; st_sp=2023-09-10%2022%3A55%3A46; st_inirUrl=https%3A%2F%2Fcn.bing.com%2F; st_sn=31; st_psi=20230926232805438-113300300815-1204665939",
            "Host": "push2.eastmoney.com",
            "Referer": "https://data.eastmoney.com/zjlx/zs000001.html",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf8'
        datas = resp.json()['data']['klines']

        time_capital_flows = []
        for data in datas:
            data = data.split(',')
            time_capital_flows.append({
                '时间': data[0],
                '主力净流入': round(float(data[1]) / 1e8, 3),
                '小单净流入': round(float(data[2]) / 1e8, 3),
                '中单净流入': round(float(data[3]) / 1e8, 3),
                '大单净流入': round(float(data[4]) / 1e8, 3),
                '超大单净流入': round(float(data[5]) / 1e8, 3)
            })
        return time_capital_flows
    
    def get_hugangtong_beixiang_nanxiang_capital_flows(self):
        """
        获取南向实时资金流
        """
        time_token = int(time.time() * 1000)
        base_url = "https://push2.eastmoney.com/api/qt/kamtbs.rtmin/get?fields1=f1,f2,f3,f4&fields2=f51,f54,f52,f58,f53,f62,f56,f57,f60,f61&ut=b2884a393a59ad64002292a3e90d46a5&_={}"
        url = base_url.format(time_token)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Connection': 'keep-alive',
            "Cookie": "qgqp_b_id=a9a259420563b8c14556a92ff2af9233; emshistory=%5B%22600852%22%2C%22600849%22%2C%22600840%22%2C%22600530%22%2C%22002799%22%2C%22000890%22%5D; HAList=ty-1-000001-%u4E0A%u8BC1%u6307%u6570%2Cty-1-600530-*ST%u4EA4%u6602%2Cty-0-002799-%u73AF%u7403%u5370%u52A1%2Cty-0-000890-%u6CD5%u5C14%u80DC%2Cty-0-000584-ST%u5DE5%u667A; websitepoptg_api_time=1695780772500; st_si=38752161192026; st_asi=delete; st_pvi=70429278548518; st_sp=2023-09-10%2022%3A55%3A46; st_inirUrl=https%3A%2F%2Fcn.bing.com%2F; st_sn=8; st_psi=20230927112621198-113200301324-6566836724",
            "Host": "push2.eastmoney.com",
            "Referer": "https://data.eastmoney.com/hsgt/index.html",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf8'
        datas = resp.json()['data']
        
        # 南向资金
        n2s = datas['n2s']
        north_to_south_flows = []
        for data in n2s:
            data = data.split(',')
            north_to_south_flows.append({
                '时间': data[0],
                '港股通(沪)净买额': round(float(data[1]) / 1e4, 3) if data[1] != '-' else None,
                '港股通(沪)买入额': round(float(data[2]) / 1e4, 3) if data[2] != '-' else None,
                '港股通(沪)卖出额': round(float(data[4]) / 1e4, 3) if data[4] != '-' else None,

                '港股通(深)净买额': round(float(data[3]) / 1e4, 3) if data[3] != '-' else None,
                '港股通(深)买入额': round(float(data[6]) / 1e4, 3) if data[6] != '-' else None,
                '港股通(深)卖出额': round(float(data[7]) / 1e4, 3) if data[7] != '-' else None,

                '南向资金净买额': round(float(data[5]) / 1e4, 3) if data[5] != '-' else None,
                '南向资金买入额': round(float(data[8]) / 1e4, 3) if data[8] != '-' else None,
                '南向资金卖出额': round(float(data[9]) / 1e4, 3) if data[9] != '-' else None
            })

        return north_to_south_flows
    
    def get_market_eval(self, market_eval_type):
        """
        获取市场的估值
        https://emrnweb.eastmoney.com/nxfxb/home

        Args:
            market_eval_type: 估值类型，支持 TTM、MRQ
        """
        market_eval_dict = {
            "TTM": '1',
            "MRQ": '2'
        }
        base_url = "https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=RPT_REVALUE_TREND&columns=TRADE_DATE,INDICATOR_TYPE,AVG_VALUE,PERCENTILE,PERCENTILE_30,PERCENTILE_50,PERCENTILE_70&filter=(INDICATOR_TYPE%3D%22{}%22)&sortTypes=1&sortColumns=TRADE_DATE&source=securities&client=APP"
        url = base_url.format(market_eval_dict[market_eval_type])

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Connection': 'keep-alive',
            "Cookie": "qgqp_b_id=a9a259420563b8c14556a92ff2af9233; HAList=ty-1-600530-*ST%u4EA4%u6602%2Cty-0-002799-%u73AF%u7403%u5370%u52A1%2Cty-0-000890-%u6CD5%u5C14%u80DC%2Cty-0-000584-ST%u5DE5%u667A; emshistory=%5B%22600852%22%2C%22600849%22%2C%22600840%22%2C%22600530%22%2C%22002799%22%2C%22000890%22%5D; st_si=06784196319334; st_pvi=70429278548518; st_sp=2023-09-10%2022%3A55%3A46; st_inirUrl=https%3A%2F%2Fcn.bing.com%2F; st_sn=1; st_psi=20230922225612320-113300301068-8678062405; st_asi=delete; JSESSIONID=F2AB4B37C418A9A9BA0F2438C200324A",
            "Host": "datacenter.eastmoney.com",
            "Origin": "https://emrnweb.eastmoney.com",
            "Referer": "https://emrnweb.eastmoney.com/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf8'
        datas = resp.json()['result']['data']

        eval_values = []
        for data in datas:
            print(data)
            eval_values.append({
                '日期': data['TRADE_DATE'].split(' ')[0],
                '估值': data['AVG_VALUE']
            })
        eval_value_30_percentile = datas[-1]['PERCENTILE_30']
        eval_value_50_percentile = datas[-1]['PERCENTILE_50']
        eval_value_70_percentile = datas[-1]['PERCENTILE_70']
        current_percentile = datas[-1]['PERCENTILE']
        return {
            "eval_values": eval_values,
            "eval_value_30_percentile": eval_value_30_percentile,
            "eval_value_50_percentile": eval_value_50_percentile,
            "eval_value_70_percentile": eval_value_70_percentile,
            "current_percentile": current_percentile
        }


    def get_all_stocks_board(self):
        """
        获取 A 股的所有股票最新排名榜单
        """
        time_token = int(time.time() * 1000)
        page_size = 6000
        fields = ','.join(EASTMONEY_A_BOARD_FIELDS.keys())
        url = "https://87.push2.eastmoney.com/api/qt/clist/get?pn=1&pz={}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=1128014811999944|0|1|0|web&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048&fields={}&_={}"
        url = url.format(page_size, fields, time_token)

        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Connection': 'keep-alive',
            "Cookie": "qgqp_b_id=5dd5a3c880eed4135c765cec5bbdf661; HAList=ty-100-NDX-%u7EB3%u65AF%u8FBE%u514B%2Cty-0-300147-%u9999%u96EA%u5236%u836F%2Cty-1-600789-%u9C81%u6297%u533B%u836F%2Cty-1-600095-%u6E58%u8D22%u80A1%u4EFD%2Cty-0-300059-%u4E1C%u65B9%u8D22%u5BCC%2Cty-1-000001-%u4E0A%u8BC1%u6307%u6570%2Cty-0-399300-%u6CAA%u6DF1300; st_si=93780881941027; st_asi=delete; st_pvi=88716095105714; st_sp=2024-01-04%2013%3A36%3A25; st_inirUrl=http%3A%2F%2F127.0.0.1%3A8080%2F; st_sn=5; st_psi=20241010170113124-113200301321-4828916229",
            "Host": "87.push2.eastmoney.com",
            "Referer": "http://quote.eastmoney.com/center/gridlist.html",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
        }
        resp = requests.get(url, headers=headers)
        resp.encoding = 'utf8'
        stocks = resp.json()['data']['diff']

        all_stocks = []
        for stock in stocks:
            stock_info = {}
            for f in stock:
                if f in EASTMONEY_A_BOARD_FIELDS:
                    stock_info[EASTMONEY_A_BOARD_FIELDS[f]] = stock[f]
            all_stocks.append(stock_info)
        all_stocks = pd.DataFrame(all_stocks)
        return all_stocks
