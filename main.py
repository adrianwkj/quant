# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import akshare as ak
import pandas as pd
import datetime
import psycopg2
from sqlalchemy import create_engine


def czsczb(x):
    if x.收盘 > x.ma233:
        return 1
    elif x.收盘 > x.ma144:
        return 2
    elif x.收盘 > x.ma89:
        return 3
    elif x.收盘 > x.ma55:
        return 4
    elif x.收盘 > x.ma34:
        return 5
    elif x.收盘 > x.ma21:
        return 6
    elif x.收盘 > x.ma13:
        return 7
    elif x.收盘 > x.ma5:
        return 8
    else:
        return 9


def stock_list():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    return stock_zh_a_spot_em_df["代码"]


def get_cszszb(symbol, conn):
    today = datetime.date.today()
    today_str = today.strftime("%Y%m%d")
    last_year_str = (today - datetime.timedelta(days=365)).strftime("%Y%m%d")

    # latest_date = pd.read_sql("select max(trade_date) as trade_date from stock_hist where symbol = %(symbol)s", conn,
    #                          params={'symbol': symbol}).trade_date.values.tolist()[0]

    # start_date = None
    # if latest_date is None:
    #     start_date = (today - datetime.timedelta(days=365)).strftime("%Y%m%d")
    # elif latest_date < today:
    #     start_date = (latest_date + datetime.timedelta(days=1)).strftime("%Y%m%d")

    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol,
                                            period="daily",
                                            start_date=last_year_str,
                                            end_date=today_str,
                                            adjust="qfq")
    stock_zh_a_hist_df["ma5"] = stock_zh_a_hist_df["收盘"].rolling(window=5).mean()
    stock_zh_a_hist_df["ma13"] = stock_zh_a_hist_df["收盘"].rolling(window=13).mean()
    stock_zh_a_hist_df["ma21"] = stock_zh_a_hist_df["收盘"].rolling(window=21).mean()
    stock_zh_a_hist_df["ma34"] = stock_zh_a_hist_df["收盘"].rolling(window=34).mean()
    stock_zh_a_hist_df["ma55"] = stock_zh_a_hist_df["收盘"].rolling(window=55).mean()
    stock_zh_a_hist_df["ma89"] = stock_zh_a_hist_df["收盘"].rolling(window=89).mean()
    stock_zh_a_hist_df["ma144"] = stock_zh_a_hist_df["收盘"].rolling(window=144).mean()
    stock_zh_a_hist_df["ma233"] = stock_zh_a_hist_df["收盘"].rolling(window=233).mean()

    stock_zh_a_hist_df["czsczb"] = stock_zh_a_hist_df.apply(lambda x: czsczb(x), axis=1)

    exist_date = pd.read_sql("select trade_date from stock_hist where symbol = %(symbol)s", conn,
                             params={'symbol': symbol})
    exist_date_list = [str(x) for x in exist_date.trade_date.values.tolist()]

    insert_data = stock_zh_a_hist_df[~stock_zh_a_hist_df.日期.isin(exist_date_list)]

    insert_data.rename(columns={'日期': 'trade_date',
                                '开盘': 'open',
                                '收盘': 'close',
                                '最高': 'high',
                                '最低': 'low',
                                '成交量': 'volume',
                                '成交额': 'turnover',
                                '振幅': 'amplitude',
                                '涨跌幅': 'change_percent',
                                '涨跌额': 'change',
                                '换手率': 'turnover_rate'
                                }, inplace=True)

    insert_data.insert(loc=1, column='symbol', value=symbol)

    insert_data.to_sql(name='stock_hist', con=conn, index=False, if_exists='append')

    today_czsczb = stock_zh_a_hist_df.iat[-1, -1]
    print(symbol)
    return today_czsczb


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # conn = psycopg2.connect(
    #     dbname="stock",
    #     user="postgres",
    #     password="postgrespw",
    #     host="127.0.0.1",
    #     port="55000"
    # )

    conn = create_engine('postgresql+psycopg2://postgres:postgrespw@localhost:55000/stock')
    # df = pd.read_sql("select * from t", conn)

    sl = stock_list()
    sl1 = sl.apply(lambda x: get_cszszb(x, conn))
    d = {
        "symbol": sl,
        "czsczb": sl1
    }
    print(pd.DataFrame(d))
    conn.close()
