# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import akshare as ak
import pandas as pd
from sqlalchemy import create_engine


def ld_index(ma_list, close):
    ma_frame = pd.DataFrame({
        'ma': ma_list
    })
    new_data = pd.DataFrame({
        'ma': [close]
    }, index=['new'])
    result = pd.concat([ma_frame, new_data], axis=0)
    result['flag'] = result.index
    result.sort_values(by='ma', ascending=False, inplace=True)
    result.reset_index(drop=True, inplace=True)
    return result[result['flag'] == 'new'].index.values[0]


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    conn = create_engine('postgresql+psycopg2://postgres:postgrespw@localhost:55000/stock')

    ma_index = [5, 13, 21, 33]
    industry = pd.read_sql("select * from industry_hist", conn)
    # industry.apply(lambda x: map(lambda y: x['close'].rolling(window=y).mean(), ma_index))
    # industry['close'].rolling(window=5).mean()
    mm = {}
    for name, group in industry.groupby('symbol'):
        group['ma5'] = group['close'].rolling(5).mean()
        group['ma13'] = group['close'].rolling(13).mean()
        group['ma21'] = group['close'].rolling(21).mean()
        group['ma34'] = group['close'].rolling(34).mean()
        group['ma55'] = group['close'].rolling(55).mean()
        group['ma89'] = group['close'].rolling(89).mean()
        group['ma144'] = group['close'].rolling(144).mean()
        group['ma233'] = group['close'].rolling(233).mean()
        a = group.apply(
            lambda x: [x['ma5'], x['ma13'], x['ma21'], x['ma34'], x['ma55'], x['ma89'], x['ma144'], x['ma233'],
                       x['close']], axis=1)
        b = group.apply(lambda x: ld_index(
            [x['ma5'], x['ma13'], x['ma21'], x['ma34'], x['ma55'], x['ma89'], x['ma144'], x['ma233']], x['close']),
                        axis=1)
        mm[name] = b.tail(1).values[0]
        mm1 = sorted(mm.items(), key=lambda x: x[1])
    print(mm1)

