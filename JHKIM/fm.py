import sys, os
import numpy as np
import pandas as pd
import pyodbc
import pymysql

class fm():
    def __init__(self):
        self.sqlInfo = 'DRIVER={SQL Server};SERVER=211.174.180.232;DATABASE=DaishinQuantDB;UID=quant;PWD=9uant'
        self.mysqlcon = pymysql.connect(host='172.17.2.100', port=3306, user='smkim', passwd='smkim', db='smk',charset='utf8')

        self.conn = pyodbc.connect(self.sqlInfo)
        self.cursor = self.conn.cursor()
        self.fnum = 1

    def univ(self, dt):
        string = "select b.TRD_DT, 'A' + b.stk_cd stk_cd, b.stk_nm_kor, d.SEC_NM_KOR 'wicsBig', e.mkt_val \
                  from WFNS2DB.dbo.TZ_DATE a, WFNS2DB.dbo.TS_STK_ISSUE b, WFNC2DB.dbo.TC_COMPANY c, WFNC2DB.dbo.TC_SECTOR d, WFNS2DB.dbo.TS_STK_DAILY e where a.ymd = b.TRD_DT and b.stk_cd = c.CMP_CD and substring(c.gics_cd,1,3) = d.SEC_CD and b.STK_CD = e.stk_cd \
	              and b.TRD_DT = e.TRD_dt and a.ymd = '%s' and b.KS200_TYP = 1 order by MKT_VAL desc" % dt

        self.cursor.execute(string)
        row = self.cursor.fetchone()

        dt = []
        code = []
        nm = []
        wics = []
        cap = []
        while row:
            dt.append(row[0])
            code.append(row[1])
            nm.append(row[2])
            wics.append(row[3])
            cap.append(row[4])
            row = self.cursor.fetchone()

        df = pd.DataFrame({'DT1': dt, 'CODE': code, 'NAME': nm, 'WICSBIG': wics, 'CAP': cap},
                          columns=['DT1', 'CODE', 'NAME', 'WICSBIG', 'CAP'])

        return df

    def factor(self, df, item):
        res = pd.read_sql("SELECT stk_cd, val FROM smk.stock_items where item_cd='%s' and period='%s'" % (item, list(set(df['DT1'].tolist()))[0]), self.mysqlcon)
        df = df.merge(res, left_on='CODE', right_on='stk_cd', how='left')

        df.rename(columns={'val': 'f%s' % self.fnum}, inplace=True)
        self.fnum += 1
        del df['stk_cd']

        return df

    def scoring(self, df, fnum):
        for i in range(fnum-1):
            df[['f%s' % str(i + 1)]] = df[['f%s' % str(i + 1)]].apply(lambda x: (x-x.mean()) / x.std())
            df[['f%s' % str(i + 1)]].fillna(0, inplace=True)

        print(df)

    # def port(self):
    # def r(self):

test = fm()
df = test.univ('20170831')
df = test.factor(df,'S102306')
df = test.factor(df,'S102306')
df = test.scoring(df, test.fnum)
print("S")
