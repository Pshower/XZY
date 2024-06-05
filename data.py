'''
这是有关数据处理的内容
'''
import pandas as pd
import numpy as np
import akshare as ak
from typing import (List, Tuple, Dict, Callable, Union)


# 获取整理的股票代码列表
def get_index_codes(type='after_del'):
    if(type=='after_del'):
        path='01_data/hs300_list.csv'
    else:
        path='01_data/hs300_list_raw.csv'
    code300 = pd.read_csv(path,dtype={'code': 'str'})     #将txt文件的所有内容读入到字符串str中
    return list(code300['code'])

# 下载需要的股票代码列表
def download_index_codes(index='000300'):
    index300_stock_info_df = ak.index_stock_cons_sina(symbol=index)
    code300=index300_stock_info_df['code']
    code300.to_csv('01_data/hs300_list_raw.csv')
    code300=code300.tolist()
    # 删除交易日较少的股票
    code300.remove('001289')
    code300.remove('301269')
    code300.remove('600938')
    code300.remove('600941')
    code300.remove('601059')
    code300.remove('601728')
    code300.remove('601868')
    code300.remove('688041')
    code300.remove('688223')
    code300.remove('688271')
    code_s=pd.Series(code300)
    code_s.name='code'
    code_s.to_csv('01_data/hs300_list.csv')
    return code300

# 下载需要的股票
def down_data(code, period='daily', from_date='20110101', to_date='20240531',fq='qfq'):
    df = ak.stock_zh_a_hist(symbol=code, period=period, start_date=from_date, end_date=to_date,adjust=fq)
    path_file = '01_data/raw/%s.csv' % (code)
    df.to_csv(path_file)
    print('下载了%s'%(code))

# 下载沪深300指数信息
def down_index(code='399300', from_date='20110101', to_date='20240531'):
    df=ak.index_hist_cni(symbol=code, start_date=from_date, end_date=to_date)
    df.to_csv('01_data/hs300_index.csv')
    print('下载了沪深300')

# 获取沪深300指数的时间线
def data_line():
    # 为了填充空白日期
    hs300=pd.read_csv('01_data/hs300_index.csv')
    lstDateTime = pd.to_datetime(list(hs300['日期']), format='%Y-%m-%d')
    lstDT_df=pd.DataFrame(lstDateTime)
    lstDT_df.columns=['date']
    lstDT_df.set_index('date',inplace=True)
    return lstDT_df

# 整理并合并数据
def clean_data(code):
    lstDT_df=data_line()
    df=pd.read_csv('01_data/raw/%s.csv' % (code),index_col=0)
    df.columns=['date','open','close','high','low','volume','value','amplitude','A-D','range','turnover']
    df['date']=pd.to_datetime(df['date'])
    df.set_index('date',inplace=True)
    df2=pd.merge(lstDT_df,df,on='date',how='outer')
    df2.fillna(method='ffill', inplace=True)
    return df2

# 获取所需数据
def get_data(code,type='after_del'):
    if(type=='after_del'):
        path='01_data/fill/'
    else:
        path='01_data/raw/'
    df=pd.read_csv(path+'%s.csv' % (code),index_col=0)
    seq=code
    df.insert(0, 'code',seq)
    df.index = pd.to_datetime(df.index)
    return df

# 获取所有数据并按照时间顺序排列
def total_data(sort_type='time'):
    if(sort_type=='time'):
        df=pd.read_csv('01_data/total.csv',index_col=0,dtype={'code': 'str'})
        df.sort_values(by='date',inplace=True,kind='stable')
    elif(sort_type=='code'):
        df=pd.read_csv('01_data/total.csv',index_col=0,dtype={'code': 'str'})
    return df

# 获取月度时间戳
def get_periods():
    periods=data_line()
    periods=pd.to_datetime(periods.index)
    days = pd.Index(periods)
    idx_df = days.to_frame()
    freq = 'ME'
    if freq[-1] == 'E':
        day_range = idx_df.resample(freq[0]).last()
    else:
        day_range = idx_df.resample(freq[0]).first()
    
    day_range = day_range['date'].dt.date
    periods=day_range.dropna().values.tolist()
    return periods


# 获取收盘价格
def get_close_price(security: Union[List, str], periods: List) -> pd.DataFrame:
    """获取对应频率价格数据

    Args:
        security (Union[List, str]): 标的
        periods (List): 频率

    Yields:
        Iterator[pd.DataFrame]
    """
    for trade in periods:

        yield get_data(security).loc[periods,'close']

