# -*- coding:utf-8 -*-
import MySQLdb
import yaml
import pandas as pd
import numpy as np
import os
import time
from orderdict_yaml_loader import OrderedDictYAMLLoader
from user_subacc import UserWriter
from acc_subacc import AccountWriter
from writer import Writer


class FundWriter(UserWriter):
    def __init__(self, config, conn):
        super(FundWriter, self).__init__(config, conn)

    def get_dfs(self, fund_code, type):
        # fund->user
        if type == 1:
            fund_sql = "select UserCode from Mapping where FundCode = {} and Type ='1'".format(fund_code)
        # fund->account
        elif type == 2:
            fund_sql = "select AccountCode from Mapping where FundCode = {} and Type = '2'".format(fund_code)
        id_df = pd.read_sql(fund_sql, self.conn)
        if id_df.empty:
            return 2

        i = 0
        for id in id_df[id_df.columns[0]]:
            if type == 1:
                user_writer = UserWriter(self.config, self.conn)
                t = user_writer.get_user_dfs(id)
            elif type == 2:
                account_writer = AccountWriter(self.config, self.conn)
                t = account_writer.get_account_dfs(id, True)
            df1 = t[0]
            df2 = t[6]
            df1 = df1.iloc[:, :11]
            df2 = df2.ix[:, ['TradingDay', 'InstrumentCode', 'LongPosition', 'ShortPosition', 'ProductCode']]
            if i == 0:
                settlement_df = df1
                position_df = df2
            else:
                settlement_df = settlement_df.add(df1, fill_value=0)
                position_df = position_df.append(df2)

            i += 1

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
                share_list.append(share_list[i - 1] + row['Deposit&Withdraw'] / networth_list[i - 1])
                networth_list.append(row['Balance'] / share_list[i])
            i += 1
        settlement_df['Share'] = share_list
        settlement_df['NetWorth'] = networth_list

        position_df['NetPosition'] = position_df['LongPosition'] - position_df['ShortPosition']

        days_df = pd.to_datetime(position_df['TradingDay']).drop_duplicates().nlargest(2)
        if not days_df.empty:
            current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
            # current_trading_day, last_trading_day = ("20160901", "20160902")
            position_gb = position_df.groupby(['TradingDay'])
            current_trading_df = position_gb.get_group(current_trading_day)
            last_trading_df = position_gb.get_group(last_trading_day)
            last_trading_df = position_gb.get_group(last_trading_day)
            current_trading_df.set_index('TradingDay', inplace=True)
            last_trading_df.set_index('TradingDay', inplace=True)

            new_df = pd.merge(current_trading_df, last_trading_df, how='outer', indicator=True).query(
                '_merge=="left_only"').drop(['_merge'], axis=1)
            old_df = pd.merge(current_trading_df, last_trading_df, how='outer', indicator=True).query(
                '_merge=="right_only"').drop(['_merge'], axis=1)

            del current_trading_df['NetPosition']
            del last_trading_df['NetPosition']
            del new_df['NetPosition']
            del old_df['NetPosition']

            current_trading_df = current_trading_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            last_trading_df = last_trading_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            new_df = new_df.groupby(['ProductCode', 'InstrumentCode']).sum()
            old_df = old_df.groupby(['ProductCode', 'InstrumentCode']).sum()
        num_settlement_df, col_settlement_df = settlement_df.shape

        his_position_df1 = position_df.groupby(['TradingDay']).sum()
        his_position_df1 = his_position_df1.reset_index()
        del his_position_df1['LongPosition']
        del his_position_df1['ShortPosition']
        del position_df['NetPosition']

        his_position_df = pd.merge(position_df, his_position_df1, on='TradingDay')
        his_position_df = his_position_df.groupby(['TradingDay', 'NetPosition', 'ProductCode', 'InstrumentCode']).sum()

        self.df_to_excel(settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, fund_code,days_df)
        if type == 1:
            self.get_users_by_fund(fund_code)
        elif type == 2:
            self.get_accounts_by_fund(fund_code)
        return settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, position_df, days_df
