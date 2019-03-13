# -*- coding:utf-8 -*-
import MySQLdb
import yaml
import pandas as pd
import numpy as np
import time
class Writer(object):

    def __init__(self, config, conn):
        self.config = config
        self.conn = conn
        self.writer = None
        excel_name = self.config['excel']['name'] + '.xlsx'
        self.writer = pd.ExcelWriter(excel_name, engine='xlsxwriter')
    def close(self):
        if self.writer:
            self.writer.save()

    def df_to_excel(self, settlement_df, current_trading_df, last_trading_df, new_df, old_df, his_position_df, code, days_df):

        current_trading_day, last_trading_day = days_df.apply(lambda x: x.strftime('%Y%m%d'))
        num_settlement_df, col_settlement_df = settlement_df.shape
        # _account_code = rename_subaccount(account_code) if not is_account else account_code

        # u表示处理unicode文本
        settlement_df.to_excel(self.writer,
                               sheet_name=u'资金_{}'.format(code),
                               index_label=[self.config['table']['settlement']['all']['TradingDay']],
                               header=map(lambda x: self.config['table']['settlement']['all'][x] if x in self.config[
                                   'table']['settlement']['all'].keys() else x, settlement_df.columns),
                               float_format='%.4f')

        if not days_df.empty:
            # shape返回一个tuple (num_index, num_column)
            offset_row = current_trading_df.shape[0] + 6
            offset_col = current_trading_df.shape[1] + 6
            current_trading_df.to_excel(self.writer,
                                        sheet_name=u'最新持仓_{}'.format(code),
                                        index_label=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                                self.config[
                                                                                                                    'table'][
                                                                                                                    'position'][
                                                                                                                    'all'].keys() else x,
                                                        current_trading_df.index.names),
                                        header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                           self.config[
                                                                                                               'table'][
                                                                                                               'position'][
                                                                                                               'all'].keys() else x,
                                                   current_trading_df.columns),
                                        startrow=2,
                                        startcol=0)
            last_trading_df.to_excel(self.writer,
                                     sheet_name=u'最新持仓_{}'.format(code),
                                     index_label=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                             self.config[
                                                                                                                 'table'][
                                                                                                                 'position'][
                                                                                                                 'all'].keys() else x,
                                                     last_trading_df.index.names),
                                     header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                        self.config[
                                                                                                            'table'][
                                                                                                            'position'][
                                                                                                            'all'].keys() else x,
                                                last_trading_df.columns),
                                     startrow=2,
                                     startcol=offset_col)
        if not days_df.empty and not new_df.empty:
            new_df.to_excel(self.writer,
                            sheet_name=u'最新持仓_{}'.format(code),
                            index_label=map(lambda x: self.config['table']['position']['all'][x] if x in self.config[
                                'table']['position']['all'].keys() else x, new_df.index.names),
                            header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                               self.config['table'][
                                                                                                   'position'][
                                                                                                   'all'].keys() else x,
                                       new_df.columns),
                            startrow=offset_row,
                            startcol=0)
        if not days_df.empty and not old_df.empty:
            old_df.to_excel(self.writer,
                            sheet_name=u'最新持仓_{}'.format(code),
                            index_label=map(lambda x: self.config['table']['position']['all'][x] if x in self.config[
                                'table']['position']['all'].keys() else x, old_df.index.names),
                            header=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                               self.config['table'][
                                                                                                   'position'][
                                                                                                   'all'].keys() else x,
                                       old_df.columns),
                            startrow=offset_row + new_df.shape[0] + 2,
                            startcol=0)

        his_position_df.to_excel(self.writer,
                                 sheet_name=u'历史持仓_{}'.format(code),
                                 index_label=map(lambda x: self.config['table']['position']['all'][x] if x in
                                                                                                         self.config[
                                                                                                             'table'][
                                                                                                             'position'][
                                                                                                             'all'].keys() else x,
                                                 his_position_df.index.names),
                                 header=map(lambda x: self.config['table']['position']['all'][x] if x in self.config[
                                     'table']['position']['all'].keys() else x, his_position_df.columns)
                                 )

        workbook = self.writer.book
        worksheet = self.writer.sheets[u'资金_{}'.format(code)]
        bold = workbook.add_format({'bold': True})
        italic = workbook.add_format({'italic': True})
        money = workbook.add_format({'num_format': u'￥#,###.##', 'align': 'right'})
        integer = workbook.add_format({'num_format': '0', 'align': 'right'})
        decimal = workbook.add_format({'num_format': '0.00', 'align': 'right'})
        percentage = workbook.add_format({'num_format': '0.0%', 'align': 'right'})
        green = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        red = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow = workbook.add_format({'bg_color': '#FFFF00'})
        worksheet.freeze_panes(1, 0)
        # worksheet.conditional_format('K{}:K{}'.format(2, num_settlement_df + 1), {'type': '3_color_scale'})
        worksheet.conditional_format('E{}:E{}'.format(2, num_settlement_df + 1), {'type': 'cell',
                                                                                  'criteria': '>=',
                                                                                  'value': 0,
                                                                                  'format': green})

        worksheet.conditional_format('E{}:E{}'.format(2, num_settlement_df + 1), {'type': 'cell',
                                                                                  'criteria': '<',
                                                                                  'value': 0,
                                                                                  'format': red})

        networth_chart = workbook.add_chart({'type': 'line'})

        networth_chart.add_series({
            'name': u'净值',
            'categories': u'=资金_{}!$A${}:$A${}'.format(code, 2, num_settlement_df + 1),
            'values': u'=资金_{}!$P${}:$P${}'.format(code, 2, num_settlement_df + 1)})
        networth_chart.set_x_axis({
            'name_font': {'size': 14, 'bold': True},
            'num_font': {'italic': True},
        })

        worksheet.insert_chart('R2', networth_chart)
        cum_return_chart = workbook.add_chart({'type': 'line'})

        cum_return_chart.add_series({
            'name': u'累计盈亏',
            'categories': u'=资金_{}!$A${}:$A${}'.format(code, 2, num_settlement_df + 1),
            'values': u'=资金_{}!$K${}:$K${}'.format(code, 2, num_settlement_df + 1)})
        cum_return_chart.set_x_axis({
            'name_font': {'size': 14, 'bold': True},
            'num_font': {'italic': True},
        })

        worksheet.insert_chart('R20', cum_return_chart)

        if not days_df.empty:
            worksheet = self.writer.sheets[u'最新持仓_{}'.format(code)]
            worksheet.write_string(0, 0, current_trading_day, bold)
            worksheet.write_string(1, 0, u'当前持仓', bold)
            worksheet.write_string(offset_row - 1, 0, u'增加的持仓', bold)
            worksheet.write_string(offset_row + new_df.shape[0] + 1, 0, u'减少的持仓', bold)

            worksheet.write_string(0, offset_col, last_trading_day, bold)
            worksheet.write_string(1, offset_col, u'上一交易日持仓', bold)

        worksheet = self.writer.sheets[u'历史持仓_{}'.format(code)]
        worksheet.freeze_panes(1, 0)
