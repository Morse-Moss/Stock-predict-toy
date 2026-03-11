# Stock Predict Toy

一个基于Flask的股票数据分析和预测系统。

## 功能特性

- 用户注册/登录系统
- 大盘数据分析
- 个股信息查询
- 涨停板分析
- 资金流向分析
- 市场估值
- 量价分析
- 龙虎榜排名
- 股票量化分析
- 股票预测

## 技术栈

- **后端**: Flask
- **数据处理**: Pandas, NumPy
- **机器学习**: TensorFlow, Scikit-learn
- **数据可视化**: ECharts, PyEcharts
- **前端**: Bootstrap, jQuery
- **数据库**: SQLite

## 安装

1. 克隆项目
```bash
git clone https://github.com/your-username/Stock-predict-toy.git
cd Stock-predict-toy
```

2. 安装依赖
```bash
pip install -r code/requirements.txt
```

## 运行

```bash
cd code
python app.py
```

访问 http://127.0.0.1:5000

## 项目结构

```
Stock-predict-toy/
├── code/
│   ├── app.py              # Flask主应用
│   ├── api.py              # API接口
│   ├── user.py             # 用户管理
│   ├── requirements.txt    # 依赖包
│   ├── service/            # 业务逻辑
│   ├── templates/          # HTML模板
│   ├── static/             # 静态资源
│   └── dataset/            # 数据集
```

## 注意事项

- 本项目仅供学习交流使用
- 股票预测结果仅供参考，不构成投资建议
- 请勿用于实际投资决策

## License

MIT
