

import backtrader as bt
import pandas as pd
# 创建一个新的数据源类，继承自 bt.feeds.PandasData
class BinanceCSVData(bt.feeds.PandasData):
    # 添加 'openinterest' 列，因为默认情况下 PandasData 类期望它存在
    lines = ('openinterest',)

    # 设置 'openinterest' 列的默认值为 0.0
    params = (('openinterest', 0.0),)
    
def load_csv(filepath):
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"File {filepath} not found.")
        return None
    except Exception as e:
        print(f"Error occurred while reading file {filepath}: {e}")
        return None

    try:
        df = df.rename(columns={'Open time': 'open_time', 'Close time': 'close_time', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms') + pd.Timedelta(hours=8)
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms') + pd.Timedelta(hours=8)
        df = df.set_index('open_time', drop=True)
    except Exception as e:
        print(f"Error occurred while processing data: {e}")
        return None
    return df
