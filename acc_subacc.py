# -*- coding:utf-8 -*-
import MySQLdb
import yaml
import pandas as pd
import numpy as np
import os
import time
from orderdict_yaml_loader import OrderedDictYAMLLoader
from writer import Writer


def rename_subaccount(subaccount):
    return '_'.join(subaccount.split('-'))


class AccountWriter(Writer):
    """docstring for AccountWriter"""

    def __init__(self, config, conn):
        super(AccountWriter, self).__init__(config, conn)

    def get_accounts_by_fund(self, fund_code):
        account_sql = "select AccountCode from Mapping where FundCode = {} and Type = '2' order by AccountCode".format(fund_code)
        account_df = pd.read_sql(account_sql, self.conn)
        for account_code in account_df['AccountCode']:
            self.get_account(account_code)

    def get_subaccount_dfs_by_user(self, user_code):
        position_sql_column = ','.join(self.config['table']['position']['raw'].keys())
        settlement_sql_column = ','.join(self.config['table']['settlement']['raw'].keys())
        subaccount_sql = "select SubAccCode from Mapping where UserCode = {} and Type = '13' order by SubAccCode".format(user_code)
        subaccount_df = pd.read_sql(subaccount_sql, self.conn)
        i=0
        for subaccount_code in subaccount_df['SubAccCode']:
            self.get_account_dfs(subaccount_code, False)
            position_sql = "select " + position_sql_column +" from SubAccountPosition where SubAccountCode = '" + subaccount_code + "'"
            settlement_sql = "select " + settlement_sql_column + " from SubAccountSettlement where SubAccountCode = '" + subaccount_code + "'"
            if i == 0:
                position_df = pd.read_sql(position_sql, self.conn)
                settlement_df = pd.read_sql(settlement_sql, self.conn, index_col='TradingDay')
            else:
                df1 = pd.read_sql(position_sql, self.conn)
                df2 = pd.read_sql(settlement_sql, self.conn, index_col='TradingDay')
                position_df = position_df.append(df1)
                settlement_df = settlement_df.add(df2, fill_value=0)
            i += 1
        return settlement_df, position_df
    #def get_accout_dfs_by_fund(self, fund_code):
        #subaccount_sql = "select SubAccountCode from Mapping where F = {} order by SubAccountCode".format()
        #subaccount_dfs = pd.read_sql(subaccount_sql, self.conn)

    def get_account(self, account_code):
        t = self.get_account_dfs(account_code, True)
        if t == 2:
            return 2
        self.get_subaccount_dfs_by_account(account_code)

    def get_subaccount_dfs_by_account(self, account_code):
        #给出一个账户，然后找出所有子账户
        subaccount_sql = "select SubAccountCode from SubAccount where AccountCode = {} order by SubAccountCode".format(account_code)
        subaccount_dfs = pd.read_sql(subaccount_sql, self.conn)
        #return map(lambda x: self.df_to_excel(self.get_account_dfs(x, False)), subaccount_dfs['SubAccountCode'])
        for subaccount_code in subaccount_dfs['SubAccountCode']:
            self.get_account_dfs(subaccount_code, False)

    def get_account_dfs(self, id, is_account):
        position_sql_column = ','.join(self.config['table']['position']['raw'].keys())
        settlement_sql_column = ','.join(self.config['table']['settlement']['raw'].keys())

        instrument_sql = "select InstrumentID, ProductCode from Instrument"

        if is_account:
            position_sql = "select {} from AccountPosition where InvestorID = '{}'".format(position_sql_column, id)
            settlement_sql = "select {} from AccountSettlement where AccountCode = '{}'".format(settlement_sql_column, id)
        else:
            position_sql = "select {} from SubAccountPosition where SubAccountCode = '{}'".format(position_sql_column, id)
            settlement_sql = "select {} from SubAccountSettlement where SubAccountCode = '{}'".format(settlement_sql_column, id)

        instrument_df = pd.read_sql(instrument_sql, self.conn)
        position_df = pd.read_sql(position_sql, self.conn)
        if instrument_df.empty:
            return 2
        elif position_df.empty:
            return 2

        settlement_df = pd.read_sql(settlement_sql, self.conn, index_col='TradingDay')
        instrument_df.rename(columns={'InstrumentID': 'InstrumentCode'}, inplace=True)
        """inplace默认为False 返回一个新的DataFrame 若inplace=True 返回处理后的原有的DataFrame"""

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
            """DataFrame.iterrows()返回的是(index, series) 其中series就是columns和values"""
            if i == 0:
                share_list.append(row['Deposit&Withdraw'])
                networth_list.append(row['Balance'] / row['Deposit&Withdraw'])
            else:
                share_list.append(share_list[i-1] + row['Deposit&Withdraw'] / networth_list[i-1])
                networth_list.append(row['Balance'] / share_list[i])
            i += 1

        settlement_df['Share'] = share_list
        settlement_df['NetWorth'] = networth_list

        """list(DataFrame)得到的是columns"""
        settlement_cols = list(settlement_df)
        settlement_cols.insert(5, settlement_cols.pop(settlement_cols.index('Deposit&Withdraw')))
        settlement_cols.insert(3, settlement_cols.pop(settlement_cols.index('NetProfit')))
        settlement_df = settlement_df.ix[:, settlement_cols]

        #LongPosition:多头仓位
        #ShortPosition:空头仓位
        position_df['LongPosition'] = position_df['Position'] * position_df['PosiDirection'].apply(lambda x: 1 if int(x) == 2 else 0)
        position_df['ShortPosition'] = position_df['Position'] * position_df['PosiDirection'].apply(lambda x: 1 if int(x) == 3 else 0)
        position_df['NetPosition'] = position_df['LongPosition'] - position_df['ShortPosition']

        del position_df['PosiDirection']
        del position_df['Position']
        position_df.replace(0, np.nan)
        position_df = pd.merge(position_df, instrument_df, on='InstrumentCode')



        """drop_duplicates(subset,keep,inplace) subset为选定的列做distinct,默认为所有列 keep 值选项{'first','last',False} 保留
        重复元素的第一个，最后一个，或者全部删除"""
        days_df = pd.to_datetime(position_df['TradingDay']).drop_duplicates().nlargest(2)
        if not days_df.empty:
            #df.apply返回一个DataFrame
            current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
            #current_trading_day, last_trading_day = ("20160901", "20160902")
            position_gb = position_df.groupby(['TradingDay'])

            current_trading_df = position_gb.get_group(current_trading_day)
            last_trading_df = position_gb.get_group(last_trading_day)
            last_trading_df = position_gb.get_group(last_trading_day)
            current_trading_df.set_index('TradingDay', inplace=True)
            last_trading_df.set_index('TradingDay', inplace=True)

            """
            表明这行来自于哪个DataFrame
            indicator=True 增加一个column ：_merge 
            indicator=str 增加一个column: str 
            value:left right both
            """
            new_df = pd.merge(current_trading_df, last_trading_df, how='outer', indicator=True).query('_merge == "left_only"').drop(['_merge'], axis=1)
            old_df = pd.merge(current_trading_df, last_trading_df, how='outer', indicator=True).query('_merge == "right_only"').drop(['_merge'], axis=1)

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

        self.df_to_excel(settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, id, days_df)
        return settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, position_df, days_df
