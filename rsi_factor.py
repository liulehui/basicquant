#  -*- coding: utf-8 -*-

from database import DB_CONN
from stock_util import get_all_codes
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback
from matplotlib import pyplot as plt


def compute_rsi(begin_date, end_date):
    codes = get_all_codes()

    # 计算RSI
    N = 12
    for code in codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算MACD
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            df_daily.set_index(['date'], 1, inplace=True)
            df_daily['pre_close'] = df_daily['close'].shift(1)
            df_daily['change_pct'] = (df_daily['close'] - df_daily['pre_close']) * 100 / df_daily['pre_close']
            # 保留上涨的日期
            df_daily['up_pct'] = DataFrame({'up_pct': df_daily['change_pct'], 'zero': 0}).max(1)

            # 计算RSI
            df_daily['RSI'] = df_daily['up_pct'].rolling(N).mean() / abs(df_daily['change_pct']).rolling(N).mean() * 100

            df_daily.plot(kind='line', title='RSI', y=['RSI'])
            plt.show()
            # 移位
            # df_daily['PREV_RSI'] = df_daily['RSI'].shift(1)
            #
            #
            #
            # # 超买，RSI下穿80
            # df_daily_gold = df_daily[(df_daily['RSI'] < 80) & (df_daily['PREV_RSI'] >= 80)]
            # # 超卖，RSI上穿20
            # df_daily_dead = df_daily[(df_daily['RSI'] > 20) & (df_daily['PREV_RSI'] <= 20)]
            #
            # # 保存结果到数据库
            # update_requests = []
            # for date in df_daily_gold.index:
            #     update_requests.append(UpdateOne(
            #         {'code': code, 'date': date},
            #         {'$set': {'code':code, 'date': date, 'signal': 'gold'}},
            #         upsert=True))
            #
            # for date in df_daily_dead.index:
            #     update_requests.append(UpdateOne(
            #         {'code': code, 'date': date},
            #         {'$set': {'code':code, 'date': date, 'signal': 'dead'}},
            #         upsert=True))
            #
            # if len(update_requests) > 0:
            #     update_result = DB_CONN['rsi'].bulk_write(update_requests, ordered=False)
            #     print('Save RSI, 股票代码：%s, 插入：%4d, 更新：%4d' %
            #           (code, update_result.upserted_count, update_result.modified_count), flush=True)
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()

def is_rsi_gold(code, date):
    count = DB_CONN['rsi'].count({'code': code, 'date': date, 'signal': 'gold'})
    return count == 1

def is_rsi_dead(code, date):
    count = DB_CONN['rsi'].count({'code': code, 'date': date, 'signal': 'dead'})
    return count == 1


if __name__ == '__main__':
    compute_rsi('2015-01-01', '2015-06-30')

