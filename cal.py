import pandas as pd
import numpy as np
import talib
from math import atan
import alphalens as al
import empyrical as ep
from typing import (List, Tuple, Dict, Callable, Union)


# 辅助函数
#计算近N日递增天数
def count_diff(df,n):
    
    diff = df.diff()
    diff[diff > 0] = 1
    diff[diff <= 0] = 0
    count = diff.rolling(window=n).sum()
    
    return count

# 计算因子函数
def process_(df):
    data  = df.copy()
    data['ma5'] = talib.MA(data['close'],timeperiod=5)
    data['ma10'] = talib.MA(data['close'],timeperiod=10)
    data['ma21'] = talib.MA(data['close'],timeperiod=21)
    data['ma55'] = talib.MA(data['close'],timeperiod=55)
    data['vol21'] = talib.MA(data['volume'],timeperiod=21)
    
    data['target2'] = data['close'].shift(-2)/data['close']  # 标签
    
    # 防止分母为零，所以加个无穷小 最后用反三角函数处理将0到无穷大 映射到0到二分之pi 
    data['factor0'] = data.apply(lambda x: atan((x['high']-x['close'])/(x['close']-x['low']+0.00000001)),axis=1)
    data['factor1'] = data.apply(lambda x: atan((x['high']-x['open'])/(x['close']-x['low']+0.00000001)),axis=1)
    data['factor2'] = data.apply(lambda x: atan((x['open']-x['low'])/(x['high']-x['close']+0.00000001)),axis=1)
    data['factor3'] = data.apply(lambda x: atan((x['close']-x['low'])/(x['high']-x['close']+0.00000001)),axis=1)
    
    data['increase5'] = count_diff(df['close'],5) # 近5日价格递增的天数
    data['increase_vol5'] = count_diff(df['volume'],5)
    
    data['return1'] = (data['close']/data['close'].shift(1)-1)*100
    data['return3'] = (data['close']/data['close'].shift(3)-1)*100
    data['return5'] = (data['close']/data['close'].shift(5)-1)*100 #5日涨幅
    
    data['bias5'] = (data['close']/data['ma5']-1)*100 #乖离率
    data['bias10'] = (data['close']/data['ma10']-1)*100
    data['bias21'] = (data['close']/data['ma21']-1)*100
    data['bias55'] = (data['close']/data['ma55']-1)*100
   
    data['bias5_'] = data['bias5']-data['bias5'].shift(1)#乖离率的1阶差分
    data['bias10_'] = data['bias10']-data['bias10'].shift(1)
    data['bias21_'] = data['bias21']-data['bias21'].shift(1)
    data['bias55_'] = data['bias55']-data['bias55'].shift(1)
    
    data['trend5'] = (data['ma5']/data['ma5'].shift(3)-1)*100 # ma5的3日变化率
    data['trend21'] = (data['ma21']/data['ma21'].shift(5)-1)*100 # ma21的5日变化率
    data['high5'] = (data['high']/data['ma5']-1)*100
    data['high10'] = (data['high']/data['ma10']-1)*100
    data['high21'] = (data['high']/data['ma21']-1)*100
    
    data['k1'] = (data['close']-data['open'])/data['close'].shift(1)*2.5 #当日阳线或阴线的长度
    data['k1_1'] = data['k1'].shift(1)
    data['k1_2'] = data['k1'].shift(2)
    data['hc'] = (data['high']/data['close']-1)*100
    data['return1_'] = data['return1']-data['return1'].shift(1)#收益率的一阶差分
    data['return3_'] = data['return3']-data['return3'].shift(1)
    data['return5_'] = data['return5']-data['return5'].shift(1)
    
    data['vol21_'] = (data['volume']/data['vol21']) 
    data['vol21_1'] = data['vol21_']-data['vol21_'].shift(1)
    data['vol21_2'] = data['vol21_']-data['vol21_'].shift(2)
    data['vol21_3'] = data['vol21_']-data['vol21_'].shift(3)
    
    return data

class get_factor_returns(object):

    def __init__(self, factors: pd.DataFrame, factor_name: str, max_loss: float) -> None:
        '''
        输入:factors MuliIndex level0-date level1-asset columns-factors
        '''
        self.factors = factors
        self.factor_name = factor_name
        self.name = self.factor_name
        self.max_loss = max_loss

    def get_calc(self, pricing: pd.DataFrame, periods: Tuple = (1,), quantiles: int = 5) -> pd.DataFrame:

        factor_ser: pd.Series = self.factors[self.factor_name]
        preprocessing_factor = al.utils.get_clean_factor_and_forward_returns(factor_ser,
                                                                             pricing,
                                                                             periods=periods,
                                                                             quantiles=quantiles,
                                                                             max_loss=self.max_loss)

        # 预处理好的因子
        self.factors_frame = preprocessing_factor

        # 分组收益
        self.group_returns = pd.pivot_table(preprocessing_factor.reset_index(
        ), index='date', columns='factor_quantile', values=1)

        # 分组累计收益
        self.group_cum_returns = ep.cum_returns(
            self.group_returns)

    def long_short(self, lower: int = 1, upper: int = 5) -> pd.Series:
        '''
        获取多空收益
        默认地分组为1,高分组为5
        '''
        try:
            self.group_returns
        except NameError:
            raise ValueError('请先执行get_calc')

        self.long_short_returns = self.group_returns[upper] - \
            self.group_returns[lower]
        self.long_short_returns.name = f'{self.name}_excess_ret'

        self.long_short_cum = ep.cum_returns(self.long_short_returns)
        self.long_short_cum.name = f'{self.name}_excess_cum'