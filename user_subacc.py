# -*- coding:utf-8 -*-
import MySQLdb
import yaml
import pandas as pd
import numpy as np
import os
import time
from orderdict_yaml_loader import OrderedDictYAMLLoader
from writer import Writer
from acc_subacc import AccountWriter

class UserWriter(AccountWriter):

    def __init__(self, config, conn):
        super(UserWriter, self).__init__(config, conn)

    def get_users_by_fund(self, fund_code):
        user_sql = "select UserCode from Mapping where FundCode = {} and Type = '1' order by UserCode".format(fund_code)
        user_df = pd.read_sql(user_sql, self.conn)
        for user_code in user_df['UserCode']:
            self.get_user_dfs(user_code)

    def get_user_dfs(self, user_code):

        instrument_sql = "select InstrumentID, ProductCode from Instrument"
        instrument_df = pd.read_sql(instrument_sql, self.conn)
        instrument_df.rename(columns={'InstrumentID': 'InstrumentCode'}, inplace=True)

        t = self.get_subaccount_dfs_by_user(user_code)
        settlement_df = t[0].iloc[:, :11]
        position_df = t[1]
        if settlement_df.empty:
            return 2
        elif position_df.empty:
            return 2
        #settlement
        num_settlement_df, col_settlement_df = settlement_df.shape
        settlement_df['Deposit&Withdraw'] = settlement_df['Deposit'] - settlement_df['Withdraw']
        settlement_df['NetProfit'] = settlement_df['PositionProfit'] + settlement_df['CloseProfit'] - settlement_df['Commission']
        settlement_df['CumNetProfit'] = settlement_df['NetProfit'].cumsum()
        settlement_df['CumCommission'] = settlement_df['Commission'].cumsum()
        settlement_df['CumNetProfit/CumCommission'] = settlement_df['CumNetProfit'] / settlement_df['CumCommission']
        settlement_df['RiskLevel'] = 1 - settlement_df['Available'] / settlement_df['Balance']

        networth_list = []
        share_list = []
        i = 0
        for index, row in settlement_df.iterrows():
            if i == 0:
                share_list.append(row['Deposit&Withdraw'])
                networth_list.append(row['Balance'] / row['Deposit&Withdraw'])
            else:
                share_list.append(share_list[i-1] + row['Deposit&Withdraw'] / networth_list[i-1])
                networth_list.append(row['Balance'] / share_list[i])
            i += 1
        settlement_df['Share'] = share_list
        settlement_df['NetWorth'] = networth_list

        #改变columns的顺序
        settlement_cols = list(settlement_df)
        settlement_cols.insert(5, settlement_cols.pop(settlement_cols.index('Deposit&Withdraw')))
        settlement_cols.insert(3, settlement_cols.pop(settlement_cols.index('NetProfit')))
        settlement_df = settlement_df.ix[:, settlement_cols]


        #position
        """
        2:多头
        3：空头
        """
        position_df['LongPosition'] = position_df['Position'] * position_df['PosiDirection'].apply(lambda x: 1 if int(x) == 2 else 0)
        position_df['ShortPosition'] = position_df['Position'] * position_df['PosiDirection'].apply(lambda x: 1 if int(x) == 3 else 0)
        position_df['NetPosition'] = position_df['LongPosition'] - position_df['ShortPosition']

        del position_df['PosiDirection']
        del position_df['Position']
        position_df.replace(0, np.nan)
        position_df = pd.merge(position_df, instrument_df, on='InstrumentCode')




        days_df = pd.to_datetime(position_df['TradingDay']).drop_duplicates().nlargest(2)
        if not days_df.empty:
            current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
            #current_trading_day, last_trading_day = ("20160901", "20160902")
            position_gb = position_df.groupby(['TradingDay'])

            current_trading_df = position_gb.get_group(current_trading_day)
            last_trading_df = position_gb.get_group(last_trading_day)
            current_trading_df.set_index('TradingDay', inplace=True)
            last_trading_df.set_index('TradingDay', inplace=True)

            new_df = pd.merge(current_trading_df, last_trading_df, how='outer', indicator=True).query('_merge == "left only"').drop(['_merge'], axis=1)
            old_df = pd.merge(current_trading_df, last_trading_df, how='outer', indicator=True).query('_merge =="right only"').drop(['_merge'], axis=1)

            del current_trading_df['NetPosition']
            del last_trading_df['NetPosition']
            del new_df['NetPosition']
            del old_df['NetPosition']

            current_trading_df = current_trading_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            last_trading_df = last_trading_df.groupby(['ProductCode', 'InstrumentCode']).sum()

            new_df = new_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            old_df = old_df.groupby(['ProductCode', 'InstrumentCode']).sum()

        his_position_df1 = position_df.groupby(['TradingDay']).sum()
        his_position_df1 = his_position_df1.reset_index()
        del his_position_df1['LongPosition']
        del his_position_df1['ShortPosition']
        del position_df['NetPosition']


        his_position_df = pd.merge(position_df, his_position_df1, on='TradingDay')
        his_position_df = his_position_df.groupby(['TradingDay', 'NetPosition', 'ProductCode', 'InstrumentCode']).sum()
        self.df_to_excel(settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, user_code, days_df)
        return settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, position_df, days_df