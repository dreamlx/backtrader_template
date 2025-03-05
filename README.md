# Binance 历史数据下载工具

这个工具用于下载和管理币安交易所的历史K线数据，支持多种交易对和时间周期。

## 功能特点

- 支持下载币安现货和期货的历史K线数据
- 支持多种时间周期：1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo
- 自动验证数据完整性和连续性
- 支持合并多个月份的数据文件
- 断点续传功能，避免重复下载

## 使用方法

### 下载数据

下载指定交易对和时间周期的历史数据：

```bash
python download_data.py --symbols BTCUSDT ETHUSDT --periods 1m 5m --start-date 2024-01-01 --end-date 2024-03-01
```

参数说明：
- `--symbols`: 交易对列表，例如 BTCUSDT ETHUSDT
- `--periods`: 时间周期列表，例如 1m 5m 1h 1d
- `--start-date`: 开始日期，格式为 YYYY-MM-DD
- `--end-date`: 结束日期，格式为 YYYY-MM-DD
- `--data-path`: 数据保存路径，默认为 "data"

### 仅验证现有数据

验证已下载数据的完整性和连续性：

```bash
python download_data.py --symbols BTCUSDT --periods 1m --verify-only
```

### 仅合并现有数据

将已下载的月度数据文件合并为单个CSV文件：

```bash
python download_data.py --symbols BTCUSDT --periods 1m --merge-only
```

## 数据格式

下载的数据包含以下列：

1. `Open time` - 开盘时间（毫秒时间戳，已转换为datetime格式）
2. `Open` - 开盘价
3. `High` - 最高价
4. `Low` - 最低价
5. `Close` - 收盘价
6. `Volume` - 成交量
7. `Close time` - 收盘时间（毫秒时间戳，已转换为datetime格式）
8. `Quote asset volume` - 报价资产成交量
9. `Number of trades` - 成交笔数
10. `Taker buy base asset volume` - Taker买入基础资产成交量
11. `Taker buy quote asset volume` - Taker买入报价资产成交量
12. `Ignore` - 忽略字段

## 数据存储结构

数据按以下结构存储：

```
data/
  ├── BTCUSDT/
  │   ├── 1m/
  │   │   ├── BTCUSDT-1m-2024-01.zip
  │   │   ├── BTCUSDT-1m-2024-02.zip
  │   │   └── ...
  │   ├── 5m/
  │   │   └── ...
  │   └── BTCUSDT-1m.csv  # 合并后的数据文件
  ├── ETHUSDT/
  │   └── ...
  └── ...
```

## 注意事项

1. 币安API有访问频率限制，过于频繁的请求可能导致IP被临时封禁
2. 下载大量历史数据可能需要较长时间，建议使用`--verify-only`选项检查数据完整性
3. 时间戳是以毫秒为单位的Unix时间戳，已在处理过程中转换为datetime格式
4. 数据验证会检查时间序列的连续性，价格的合理性等