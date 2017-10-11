import sys, os
import numpy as np
import pandas as pd
import pyodbc
import pymysql

sqlInfo = 'DRIVER={SQL Server};SERVER=211.174.180.232;DATABASE=DaishinQuantDB;UID=quant;PWD=9uant'

dirname = os.path.dirname(os.path.abspath(__file__))
path2 = os.path.join(dirname, 'xlsxwriter')
sys.path.append(path2)
import xlsxwriter

mysqlcon = pymysql.connect(host='172.17.2.100', port=3306, user='smkim', passwd='smkim', db='smk',charset='utf8')

startDt = '20150331'
conn = pyodbc.connect(sqlInfo)
cursor = conn.cursor()
string = "select * from (select top 1 YMD from wfns2db.dbo.tz_date where TRADE_YN='1' and MNO_OF_YR in ('3','5','8','11') AND MN_END_YN='1' AND YMD<'%s' order by YMD desc) a \
          union all select YMD from wfns2db.dbo.tz_date where TRADE_YN='1' and MNO_OF_YR in ('3','5','8','11') AND MN_END_YN='1' AND YMD>='%s' union all select * from (select top 1 ymd from wfns2db.dbo.tz_date where trade_yn='1' order by dt desc) a" % (startDt, startDt)

cursor.execute(string)
row = cursor.fetchone()
dtList = []
while row:
    dtList.append(row[0])
    row = cursor.fetchone()

res = pd.read_sql("SELECT * FROM smk.stk_hist", mysqlcon)
res = res.set_index(['period','stk_cd']).unstack(1)
res.columns = res.columns.droplevel(0)
res.fillna(method='ffill', inplace=True)

stockPrice = res.copy().transpose()

for i in range(len(dtList)):
    if i == 0 or i == len(dtList)-1:
        continue
    string  = "select aaa.* from (select aa.*, rank() over (partition by aa.trd_dt, aa.wicsBig order by aa.mkt_val desc) 'no' from (select b.TRD_DT, 'A' + b.stk_cd stk_cd, b.stk_nm_kor, d.SEC_NM_KOR 'wicsBig', e.mkt_val from WFNS2DB.dbo.TZ_DATE a \
                , WFNS2DB.dbo.TS_STK_ISSUE b, WFNC2DB.dbo.TC_COMPANY c, WFNC2DB.dbo.TC_SECTOR d, WFNS2DB.dbo.TS_STK_DAILY e where a.ymd = b.TRD_DT and b.stk_cd = c.CMP_CD and substring(c.gics_cd,1,3) = d.SEC_CD and b.STK_CD = e.stk_cd \
               and b.TRD_DT = e.TRD_dt and a.mn_end_yn = 1 and a.ymd = '%s' and SUBSTRING(a.ymd, 5,2) in ('03', '05', '08', '11') and b.KS200_TYP = 1) aa) aaa \
               where aaa.no <= 5 order by aaa.TRD_DT, aaa.wicsbig, aaa.MKT_VAL desc" % dtList[i]

    cursor.execute(string)
    row = cursor.fetchone()

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
        row = cursor.fetchone()

    df = pd.DataFrame({'DT1': dt, 'CODE': code, 'NAME': nm, 'WICSBIG': wics, 'CAP': cap},
                      columns=['DT1', 'CODE', 'NAME', 'WICSBIG', 'CAP'])

    # 팩터
    res = pd.read_sql("SELECT * FROM smk.stk_hist where period='%s'" % dtList[i-1], mysqlcon)
    res.columns = ['DT', 'CODE', 'PRICE1']
    df = df.merge(res[['CODE','PRICE1']], left_on=['CODE'], right_on=['CODE'], how='left')
    res = pd.read_sql("SELECT * FROM smk.stk_hist where period='%s'" % dtList[i], mysqlcon)
    res.columns = ['DT', 'CODE', 'PRICE2']
    df = df.merge(res[['CODE', 'PRICE2']], left_on=['CODE'], right_on=['CODE'], how='left')
    # print(df['PRICE1'].isnull().values.any())
    # print(df.isnull().sum())
    df['FACTOR'] = (df['PRICE2'] / df['PRICE1'] - 1) * 100
    df.dropna(axis=0, how='any', inplace=True)
    df['RANK'] = df.groupby(['WICSBIG']).rank(axis=0, ascending=False)['CAP']
    df = df.loc[df['RANK']<=2]
    df = df.reset_index(drop=True)

    del df['PRICE1']
    del df['PRICE2']


    # 수익률
    # res = pd.read_sql("SELECT * FROM smk.stk_hist where period='%s'" % dtList[i], mysqlcon)
    # res.columns = ['DT', 'CODE', 'PRICE1']
    # df = df.merge(res[['CODE', 'PRICE1']], left_on=['CODE'], right_on=['CODE'])
    #
    # res = pd.read_sql("SELECT * FROM smk.stk_hist where period='%s'" % dtList[i+1], mysqlcon)
    # res.columns = ['DT2', 'CODE', 'PRICE2']
    # df = df.merge(res, left_on=['CODE'], right_on=['CODE'])
    # print(stockPrice[[dtList[i]]].index)
    temp = stockPrice[[dtList[i]]]
    temp2 = stockPrice[[dtList[i+1]]]
    df['DT2'] = dtList[i+1]
    df['PRICE1'] = None
    df['PRICE2'] = None
    for c in df['CODE'].tolist():
        df.loc[df['CODE'] == c, 'PRICE1'] = float(temp.loc[temp.index == c, dtList[i]])
        df.loc[df['CODE'] == c, 'PRICE2'] = float(temp2.loc[temp.index == c, dtList[i+1]])

    df = df[['DT1', 'DT2', 'CODE', 'NAME', 'WICSBIG', 'CAP', 'FACTOR', 'PRICE1', 'PRICE2', 'RANK']]
    print(dtList[i], dtList[i+1])
    print(df)

    break
print(dtList)
    # print(len(df.index))
    # print(df['WICSBIG'].drop_duplicates())
    # print(df['WICSBIG'].unique())
