# -*- coding: utf-8 -*-
from db import config, database
from user_subacc import UserWriter
from acc_subacc import AccountWriter
from fund import FundWriter
from flask import Flask, render_template,request, send_from_directory, send_file, abort, redirect, url_for
import time
import os
import base64

app = Flask(__name__)

UPLOAD_FOLDER = 'C:\\Users\\admin\\PycharmProjects\\project2'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


#根据HTTP规范，GET用于信息获取，POST表示可能修改变服务器上的资源的请求
@app.route('/', methods=['GET', 'POST'])
def download():
    error = None
    if request.method == 'POST':

        code = request.form['code']
        type = int(request.form['type'])

        db = database(config)
        conn = db.get_conn()
        if request.form['code'] == '':
            error = 'Invalid code'

        # fund->user
        elif type==1:
            f = FundWriter(config, conn)
            t = f.get_dfs(code, 1)
            if t == 2:
                error = 'Invalid code'
                f.close()
            else:
                f.close()
                return send_from_directory(app.config['UPLOAD_FOLDER'], 'demo.xlsx', as_attachment=True)

        # fund->account
        elif type==2:
            f = FundWriter(config, conn)
            t = f.get_dfs(code, 2)
            if t == 2:
                error = 'Invalid code'
                f.close()
            else:
                f.close()
                return send_from_directory(app.config['UPLOAD_FOLDER'], 'demo.xlsx', as_attachment=True)

        # user
        elif type==3:
            u = UserWriter(config, conn)
            t = u.get_user_dfs(code)
            if t == 2:
                error = 'Invalid code'
                u.close()
            else:
                u.close()
                return send_from_directory(app.config['UPLOAD_FOLDER'], 'demo.xlsx', as_attachment=True)
        # account
        elif type==4:
            a = AccountWriter(config, conn)
            t = a.get_account(code)
            if t == 2:
                error = 'Invalid code'
                a.close()
            else:
                a.close()
                return send_from_directory(app.config['UPLOAD_FOLDER'], 'demo.xlsx', as_attachment=True)

        # subaccount
        elif type==5:
            a = AccountWriter(config, conn)
            t = a.get_account_dfs(code, False)
            if t == 2:
                error = 'Invalid code'
                a.close()
            else:
                a.close()
                return send_from_directory(app.config['UPLOAD_FOLDER'], 'demo.xlsx', as_attachment=True)
            
        else:
            error = 'Invalid type'

    return render_template('get_excel.html', error=error)

if __name__ == '__main__':
    app.run()
