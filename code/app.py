#!/usr/bin/python
# coding=utf-8
from flask import Flask, render_template
from user import user_blueprint, is_login
from api import api_blueprint
import functools

app = Flask(__name__, template_folder='templates', static_folder='static')





def check_login_wrapper(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if not is_login():
            return render_template('index.html')
        else:
            return fn(*args, **kwargs)

    return wrapper


# 页面跳转
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/register')
def register():
    return render_template('register.html')


@app.route('/login')
def login():
    return render_template('login.html')


@app.route('/dapan')
@check_login_wrapper
def dapan():
    return render_template('dapan.html')

@app.route('/stock_info')
@check_login_wrapper
def stock_info():
    return render_template('stock_info.html')


@app.route('/limitup')
@check_login_wrapper
def limitup():
    return render_template('limitup.html')


@app.route('/money_flow')
@check_login_wrapper
def money_flow():
    return render_template('money_flow.html')


@app.route('/market_eval')
@check_login_wrapper
def market_eval():
    return render_template('market_eval.html')


@app.route('/volume_price')
@check_login_wrapper
def volume_price():
    return render_template('volume_price.html')


@app.route('/lhb_rank')
@check_login_wrapper
def lhb_rank():
    return render_template('lhb_rank.html')


@app.route('/stock_quant')
@check_login_wrapper
def stock_quant():
    return render_template('stock_quant.html')


@app.route('/stock_predict')
@check_login_wrapper
def stock_predict():
    return render_template('stock_predict.html')


# API 接口注册
app.register_blueprint(user_blueprint, url_prefix='/user')
app.register_blueprint(api_blueprint, url_prefix='/api')

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=False)
