#!/usr/bin/python
# coding=utf-8
import sqlite3
from flask import jsonify, Blueprint

user_blueprint = Blueprint('user', __name__)

login_name = None


# --------------- API 接口 ---------------
def is_login():
    return login_name is not None


@user_blueprint.route('/check_login')
def check_login():
    """判断用户是否登录"""
    return jsonify({'username': login_name, 'login': is_login()})


@user_blueprint.route('/register/<name>/<password>')
def register(name, password):
    conn = sqlite3.connect('user_info.db')
    cursor = conn.cursor()

    check_sql = "SELECT * FROM sqlite_master where type='table' and name='user'"
    cursor.execute(check_sql)
    results = cursor.fetchall()
    # 数据库表不存在
    if len(results) == 0:
        # 创建数据库表
        sql = """
                CREATE TABLE user(
                    name CHAR(256),
                    password CHAR(256)
                );
                """
        cursor.execute(sql)
        conn.commit()
        print('创建数据库表成功！')

    # 判断该用户是否已经被注册
    sql = "select * from user where name='{}'".format(name)
    cursor.execute(sql)
    results = cursor.fetchall()
    if len(results) > 0:
        return jsonify({'info': '该用户名已被注册！', 'success': False})
        
    sql = "INSERT INTO user (name, password) VALUES (?,?);"
    cursor.executemany(sql, [(name, password)])
    conn.commit()
    return jsonify({'info': '用户注册成功！', 'success': True})


@user_blueprint.route('/login/<name>/<password>')
def login(name, password):
    global login_name
    conn = sqlite3.connect('user_info.db')
    cursor = conn.cursor()

    sql = "select * from user where name='{}' and password='{}'".format(name, password)
    cursor.execute(sql)
    results = cursor.fetchall()

    if len(results) > 0:
        login_name = name
        return jsonify({'info': name + '用户登录成功！', 'success': True})
    else:
        return jsonify({'info': '用户名或密码错误！', 'success': False})


@user_blueprint.route('/logout')
def logout():
    """用户登出"""
    global login_name
    resp = {'info': login_name + '用户已退出登录！', 'success': True}
    login_name = None
    return jsonify(resp)
