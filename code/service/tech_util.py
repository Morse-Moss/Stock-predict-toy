#!/usr/bin/python
# coding=utf-8
import pandas as pd
import numpy as np


def AVEDEV(seq: pd.Series, N):
    """
    平均绝对偏差 mean absolute deviation

    之前用mad的计算模式依然返回的是单值
    """
    return seq.rolling(N).apply(lambda x: (np.abs(x - x.mean())).mean(), raw=True)


def MA(seq: pd.Series, N):
    return seq.rolling(N).mean()


def SMA(seq: pd.Series, N, M=1):
    """
    威廉SMA算法
    https://www.joinquant.com/post/867
    """
    if not isinstance(seq, pd.Series):
        seq = pd.Series(seq)
    ret = []
    i = 1
    length = len(seq)
    # 跳过X中前面几个 nan 值
    while i < length:
        if np.isnan(seq.iloc[i]):
            i += 1
        else:
            break
    preY = seq.iloc[i]  # Y'
    ret.append(preY)
    while i < length:
        Y = (M * seq.iloc[i] + (N - M) * preY) / float(N)
        ret.append(Y)
        preY = Y
        i += 1
    return pd.Series(ret, index=seq.tail(len(ret)).index)


def KDJ(data, N=3, M1=3, lower=20, upper=80):
    # 假如是计算kdj(9,3,3),那么，N是9，M1是3，3
    data['llv_low'] = data['low'].rolling(N).min()
    data['hhv_high'] = data['high'].rolling(N).max()
    data['rsv'] = (data['close'] - data['llv_low']) / (data['hhv_high'] - data['llv_low'])
    data['k'] = data['rsv'].ewm(adjust=False, alpha=1 / M1).mean()
    data['d'] = data['k'].ewm(adjust=False, alpha=1 / M1).mean()
    data['j'] = 3 * data['k'] - 2 * data['d']
    data['pre_j'] = data['j'].shift(1)
    data['long_signal'] = np.where((data['pre_j'] < lower) & (data['j'] >= lower), 1, 0)
    data['short_signal'] = np.where((data['pre_j'] > upper) & (data['j'] <= upper), -1, 0)
    data['signal'] = data['long_signal'] + data['short_signal']
    return {'k': data['k'].fillna(0).to_list(),
            'd': data['d'].fillna(0).to_list(),
            'j': data['j'].fillna(0).to_list()}


def EMA(seq: pd.Series, N):
    return seq.ewm(span=N, min_periods=N - 1, adjust=True).mean()


def MACD(CLOSE, short=12, long=26, mid=9):
    """
    MACD CALC
    """
    DIF = EMA(CLOSE, short) - EMA(CLOSE, long)
    DEA = EMA(DIF, mid)
    MACD = (DIF - DEA) * 2
    return {
        'DIF': DIF.fillna(0).to_list(),
        'DEA': DEA.fillna(0).to_list(),
        'MACD': MACD.fillna(0).to_list()
    }