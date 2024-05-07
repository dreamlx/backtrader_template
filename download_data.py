from datetime import datetime
from heapq import merge
from dateutil.relativedelta import relativedelta
import requests
import shutil
import pandas as pd
from logbook import Logger
import logbook
from zipfile import ZipFile
import time
import os
import itertools
import re


log = Logger('binance_data.py', level=logbook.INFO)

columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']


def download_spot_klines(symbol, period, date: datetime, path):
    """
    下载现货K线历史行情数据
    :param symbol: 交易对
    :param period: 时间周期，比如：1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo
    :param date: 日期
    :param path: 文件保存路径
    """

    # https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2021-06.zip


    requests.adapters.DEFAULT_RETRIES = 10 # 增加重连次数
    s = requests.session()
    s.keep_alive = False # 关闭多余连接


    # https://data.binance.vision/data/futures/cm/monthly/klines/AAVEUSD_PERP/

    if re.match(r'\w+USDT', symbol):
        url = 'https://data.binance.vision/data/spot/monthly/klines/%s/%s/%s-%s-%s-%02d.zip' % (symbol, period, symbol, period, date.year, date.month)
    else:
        url = 'https://data.binance.vision/data/futures/cm/monthly/klines/%s/%s/%s-%s-%s-%02d.zip' % (symbol, period, symbol, period, date.year, date.month)

    log.info(url)
    try:
        with s.get(url, stream=True) as r:
            size = 0  # 已下载文件的大小
            chunk_size = 1024 * 1024  # 每次下载数据的大小:单位字节 1024:1KB 1024*1024:1MB
            content_size = int(r.headers["content-length"])  # 文件总大小:单位字节
            
            if r.status_code == 200:
                with open(path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
    except Exception as e:
        log.warning('download error',e, path)

    return path


def download_spot_klines_range(symbol, period, start: datetime, end: datetime, dir=''):
    """
    下载指定时间范围的币安现货K线历史行情数据
    :param symbol: 交易对
    :param period: 时间周期，比如：1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo
    :param start: 开始日期
    :param end: 结束日期
    :param dir: 数据存放目录
    """

    # ETHUSDT/5m/ETHUSDT-5m-2017-11.zip

    if len(dir) > 0 and not dir.endswith('/'):
        dir += '/'

    os.makedirs('%s%s/%s' % (dir, symbol, period), exist_ok=True)

    while True:

        path = '%s%s/%s/%s-%s-%s-%02d.zip' % (dir, symbol, period, symbol, period, start.year, start.month)
        
        print(path)
        
        if not os.path.exists(path):

            download_spot_klines(symbol, period, start, path)

            print('downloading...')
            time.sleep(2)

        else:
            print('skip')

        start = start + relativedelta(months=+1)

        if start > end:
            break

        

def read_history_file(zip_file, csv_file) -> pd.DataFrame:
    """
    读取历史数据文件
    :param zip_file: zip文件路径
    :param csv_file: csv文件路径
    """
    # ETHUSDT-5m-2017-08.csv
    
    try:
        zf = ZipFile(zip_file)
        file = zf.open(csv_file)
    except:
        raise Exception('BadZipFile', zip_file)
    
    df = pd.read_csv(file, header=None, names=columns)
    file.close()
    zf.close()
    return df


def merge_history_file(dir, output) -> pd.DataFrame:
    """
    合并历史数据
    :param dir: 历史数据文件存放目录
    :param output: 合并文件输出路径
    """
    merge_df = None

    for file in os.listdir(dir):
        log.info('merge %s to %s' % (file, output))

        df = read_history_file(dir + '/' + file, file.replace('.zip', '.csv'))

        merge_df = df if merge_df is None else pd.concat([merge_df, df])
        merge_df = merge_df[merge_df['Open']!='open']
    merge_df.to_csv(output, index=False)

    return merge_df


def download_symbol(symbol, period):
    # 开始时间就这样, 没法更早了
    download_spot_klines_range(symbol, period, start=datetime(2017, 10, 1), end=datetime(2022, 11, 10), dir=data_path)
    symbol_path = os.path.join(data_path, symbol, period)
    merge_file_path = os.path.join(data_path, symbol, '{}-{}.csv'.format(symbol, period))
    try:
        merge_history_file(symbol_path, merge_file_path)
    except Exception as inst:
        x, y = inst.args 
        print(x, y)

    return merge_file_path

if __name__ == '__main__':

    base_path = os.path.abspath('.')
    data_path = os.path.join(base_path, 'data')

    symbols  = [ 'UNIUSDT', 'UNIUSD_PERP', ]#'DOTUSD_PERP', 'ETHUSD_PERP', 'BTCUSD_PERP', 'EOSUSDT', 'AAVEUSDT', 'ETCUSDT', 'ETHUSDT', 'UNIUSDT', 'BTCUSDT', 'XMRUSDT', 'XLMUSDT', 'BNBUSDT']
    periods  = ['1m','5m', '15m', '1h', '4h', '1d', '1w'] 

    

    for (symbol, period) in itertools.product(symbols, periods):
        download_symbol(symbol, period)
        time.sleep(5)
    
