# -*- coding: utf-8 -*-
# @Author  : Jason Woo
# @Time    : 2022/9/26 11:32
# @File    : get_stock_data.py
# @Description :

import akshare as ak
import pandas as pd
import datetime
from sqlalchemy import create_engine
from pandarallel import pandarallel


def stock_list():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_zh_a_spot_em_df.rename(columns={'代码': 'symbol'}, inplace=True)
    return stock_zh_a_spot_em_df[['symbol']]


def download_and_insert(symbol, period, latest_trade_datetime, conn):
    now = datetime.datetime.now()
    today = datetime.date.today()
    if now.hour <= 16:
        today = today - datetime.timedelta(days=1)
    today_str = today.strftime("%Y%m%d")
    start_date = (today - datetime.timedelta(days=5))

    if latest_trade_datetime is not None:
        if latest_trade_datetime.date() >= today:
            return 0
        else:
            start_date = (latest_trade_datetime + datetime.timedelta(days=1))

    start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    print(symbol)

    stock_zh_a_hist_min_df = ak.stock_zh_a_hist_min_em(symbol=symbol,
                                                       period=period,
                                                       start_date=start_date_str,
                                                       end_date=today_str,
                                                       adjust="qfq")

    if len(stock_zh_a_hist_min_df.index != 0):
        stock_zh_a_hist_min_df.rename(columns={'时间': 'trade_datetime',
                                               '开盘': 'open',
                                               '收盘': 'close',
                                               '最高': 'high',
                                               '最低': 'low',
                                               '最新价': 'latest',
                                               '成交量': 'volume',
                                               '成交额': 'turnover',
                                               '振幅': 'amplitude',
                                               '涨跌幅': 'change_percent',
                                               '涨跌额': 'change',
                                               '换手率': 'turnover_rate'
                                               }, inplace=True)

        stock_zh_a_hist_min_df.insert(loc=1, column='symbol', value=symbol)
        stock_zh_a_hist_min_df.insert(loc=2, column='period', value=period)
        stock_zh_a_hist_min_df.to_sql(name='stock_hist_minute', con=conn, index=False, if_exists='append')
        print("symbol: " + symbol + " " + today_str + " inserted!")
        return 1


if __name__ == '__main__':
    pandarallel.initialize(nb_workers=4)
    conn = create_engine('postgresql+psycopg2://postgres:postgrespw@localhost:55000/stock')

    period = '1'

    sl = stock_list()
    sl.set_index('symbol', inplace=True)

    current_sl = pd.read_sql(
        "select symbol, max(trade_datetime) as trade_datetime from stock_hist_minute group by symbol",
        conn)
    current_sl.set_index('symbol', inplace=True)

    final_sl = sl.join(current_sl, on='symbol', how='left')
    final_sl['symbol'] = final_sl.index
    final_sl = final_sl.where(final_sl.notnull(), None)
    # final_sl.apply(lambda x: download_and_insert(x['symbol'], period, x['trade_datetime'], conn), axis=1)
    final_sl.parallel_apply(lambda x: download_and_insert(x['symbol'], period, x['trade_datetime'], conn), axis=1)

    print("Get Data Success!")
