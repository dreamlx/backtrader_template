from datetime import datetime
from heapq import merge
from dateutil.relativedelta import relativedelta
import requests
import shutil
import pandas as pd
# from logbook import Logger
# import logbook
from zipfile import ZipFile
import time
import os
import itertools
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List


# log = Logger('binance_data.py', level=logbook.INFO)

columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']

@dataclass
class DownloaderConfig:
    """Configuration for data downloader"""
    base_url: str = "https://data.binance.vision/data"
    retry_count: int = 3
    retry_delay: int = 2
    chunk_size: int = 1024 * 1024
    data_path: Path = Path("data")
    columns: List[str] = None
    min_rows_per_file: int = 1000  # 最小期望行数
    expected_timeframe: dict = None  # 期望的时间间隔

    def __post_init__(self):
        if self.columns is None:
            self.columns = [
                'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Close time', 'Quote asset volume', 'Number of trades',
                'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
            ]
        if self.expected_timeframe is None:
            self.expected_timeframe = {
                '1m': pd.Timedelta(minutes=1),
                '5m': pd.Timedelta(minutes=5),
                '15m': pd.Timedelta(minutes=15),
                '1h': pd.Timedelta(hours=1),
                '4h': pd.Timedelta(hours=4),
                '1d': pd.Timedelta(days=1),
            }

class BinanceDataDownloader:
    """Binance historical data downloader"""
    
    def __init__(self, config: Optional[DownloaderConfig] = None):
        self.config = config or DownloaderConfig()
        self.logger = logging.getLogger(__name__)
        self._setup_session()
    
    def _setup_session(self) -> None:
        """Setup requests session with proper configuration"""
        self.session = requests.Session()
        self.session.keep_alive = False
        adapter = requests.adapters.HTTPAdapter(max_retries=self.config.retry_count)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def download_spot_klines(self, symbol: str, period: str, date: datetime, path: Path) -> bool:
        """
        Download spot kline historical data
        
        Args:
            symbol: Trading pair
            period: Time period (1m, 3m, 5m, etc.)
            date: Target date
            path: Save path
        
        Returns:
            bool: True if download successful, False otherwise
        """
        url = self._build_url(symbol, period, date)
        self.logger.info(f"Downloading from {url}")
        
        try:
            with self.session.get(url, stream=True) as response:
                if response.status_code == 200:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with open(path, 'wb') as f:
                        shutil.copyfileobj(response.raw, f)
                    return True
                else:
                    self.logger.error(f"Download failed with status code: {response.status_code}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return False

    def _build_url(self, symbol: str, period: str, date: datetime) -> str:
        """Build download URL based on parameters"""
        base_path = "spot/monthly/klines" if symbol.endswith("USDT") else "futures/cm/monthly/klines"
        return f"{self.config.base_url}/{base_path}/{symbol}/{period}/{symbol}-{period}-{date.year}-{date.month:02d}.zip"

    def read_klines_file(self, zip_path: Path) -> pd.DataFrame:
        """
        Read kline data from zip file
        
        Args:
            zip_path: Path to the zip file
            
        Returns:
            pd.DataFrame: DataFrame containing kline data
            
        Raises:
            ValueError: If file format is invalid
            FileNotFoundError: If file doesn't exist
        """
        if not zip_path.exists():
            raise FileNotFoundError(f"File not found: {zip_path}")

        try:
            with ZipFile(zip_path) as zf:
                # Get the CSV filename from the zip
                csv_name = next(name for name in zf.namelist() if name.endswith('.csv'))
                with zf.open(csv_name) as csv_file:
                    df = pd.read_csv(
                        csv_file,
                        header=None,
                        names=self.config.columns
                    )
                    
            # Basic data validation
            self._validate_klines_data(df, period)
            
            # Convert timestamp columns to datetime
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
            df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
            
            # Convert numeric columns
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            df[numeric_columns] = df[numeric_columns].astype(float)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading file {zip_path}: {str(e)}")
            raise

    def _validate_klines_data(self, df: pd.DataFrame, period: str) -> None:
        """
        增强的数据验证功能
        
        Args:
            df: DataFrame to validate
            period: Time period (1m, 5m, etc.)
            
        Raises:
            ValueError: If data validation fails
        """
        # 基础验证
        missing_cols = set(self.config.columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
            
        if df.empty:
            raise ValueError("Empty dataframe")
            
        if df.isnull().any().any():
            self.logger.warning("Dataset contains missing values")

        # 数据完整性检查
        if len(df) < self.config.min_rows_per_file:
            self.logger.warning(f"Data file contains fewer rows than expected: {len(df)}")

        # 检查时间序列的连续性
        if period in self.config.expected_timeframe:
            expected_interval = self.config.expected_timeframe[period]
            time_diffs = df['Open time'].diff()
            invalid_intervals = time_diffs[time_diffs != expected_interval]
            
            if not invalid_intervals.empty:
                self.logger.warning(
                    f"Found {len(invalid_intervals)} irregular time intervals in data. "
                    f"Expected {expected_interval}, found gaps at: {invalid_intervals.index.tolist()}"
                )

        # 检查价格数据的合理性
        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            if (df[col] <= 0).any():
                self.logger.warning(f"Found non-positive values in {col} column")
            
        # 检查High/Low价格的合理性
        invalid_hl = df[df['High'] < df['Low']]
        if not invalid_hl.empty:
            self.logger.warning(f"Found {len(invalid_hl)} records where High price is lower than Low price")

    def merge_klines_files(self, symbol: str, period: str, output_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Merge all kline files for a symbol and period
        
        Args:
            symbol: Trading pair symbol
            period: Time period
            output_path: Optional path to save merged data
            
        Returns:
            pd.DataFrame: Merged kline data
        """
        input_dir = self.config.data_path / symbol / period
        if not input_dir.exists():
            raise FileNotFoundError(f"Directory not found: {input_dir}")
            
        # Get all zip files and sort them
        zip_files = sorted(input_dir.glob("*.zip"))
        if not zip_files:
            raise FileNotFoundError(f"No zip files found in {input_dir}")
            
        merged_df = pd.DataFrame()
        total_files = len(zip_files)
        
        for i, zip_file in enumerate(zip_files, 1):
            self.logger.info(f"Processing file {i}/{total_files}: {zip_file.name}")
            df = self.read_klines_file(zip_file)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        
        # Sort by timestamp and remove duplicates
        merged_df = merged_df.sort_values('Open time').drop_duplicates(subset=['Open time'])
        
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            merged_df.to_csv(output_path, index=False)
            self.logger.info(f"Merged data saved to {output_path}")
            
        return merged_df

    def verify_data_integrity(self, symbol: str, period: str) -> bool:
        """
        验证已下载数据的完整性
        
        Args:
            symbol: Trading pair symbol
            period: Time period
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        try:
            df = self.merge_klines_files(symbol, period)
            
            # 检查时间范围的完整性
            start_time = df['Open time'].min()
            end_time = df['Open time'].max()
            expected_periods = (end_time - start_time) / self.config.expected_timeframe[period]
            actual_periods = len(df)
            
            if actual_periods < expected_periods * 0.95:  # 允许5%的缺失
                self.logger.warning(
                    f"Data might be incomplete. Expected ~{expected_periods:.0f} records, "
                    f"but found {actual_periods}"
                )
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error verifying data integrity: {str(e)}")
            return False

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
    download_spot_klines_range(symbol, period, start=datetime(2020, 1, 1), end=datetime(2025, 3, 1), dir=data_path)
    symbol_path = os.path.join(data_path, symbol, period)
    merge_file_path = os.path.join(data_path, symbol, '{}-{}.csv'.format(symbol, period))
    try:
        merge_history_file(symbol_path, merge_file_path)
    except Exception as inst:
        x, y = inst.args 
        print(x, y)

    return merge_file_path

def main():
    """Command line interface for the downloader"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Binance Historical Data Downloader')
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT'], 
                       help='Trading pairs to download (e.g., BTCUSDT ETHUSDT)')
    parser.add_argument('--periods', nargs='+', default=['1m'],
                       help='Time periods to download (e.g., 1m 5m 1h)')
    parser.add_argument('--start-date', type=str, default='2020-01-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2024-03-01',
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--data-path', type=str, default='data',
                       help='Path to store downloaded data')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify existing data without downloading')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize downloader
    config = DownloaderConfig(data_path=Path(args.data_path))
    downloader = BinanceDataDownloader(config)
    
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    for symbol, period in itertools.product(args.symbols, args.periods):
        if args.verify_only:
            print(f"Verifying data for {symbol}-{period}...")
            if downloader.verify_data_integrity(symbol, period):
                print(f"Data integrity check passed for {symbol}-{period}")
            else:
                print(f"Data integrity check failed for {symbol}-{period}")
            continue
        
        # Download data
        current_date = start_date
        while current_date <= end_date:
            path = config.data_path / symbol / period / f"{symbol}-{period}-{current_date.year}-{current_date.month:02d}.zip"
            if not path.exists():
                success = downloader.download_spot_klines(symbol, period, current_date, path)
                if success:
                    time.sleep(2)  # Rate limiting
            current_date += relativedelta(months=1)
        
        # Merge and verify downloaded data
        try:
            output_path = config.data_path / symbol / f"{symbol}-{period}.csv"
            merged_df = downloader.merge_klines_files(symbol, period, output_path)
            print(f"Successfully merged data for {symbol}-{period}")
            
            if downloader.verify_data_integrity(symbol, period):
                print(f"Data integrity check passed for {symbol}-{period}")
            else:
                print(f"Data integrity check failed for {symbol}-{period}")
                
        except Exception as e:
            print(f"Error processing data for {symbol}-{period}: {str(e)}")

if __name__ == '__main__':
    main()
    
