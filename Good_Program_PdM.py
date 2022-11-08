import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import pymssql as ps
from datetime import datetime, timedelta
import threading
import schedule
import time

def connection80():
    host80 = '10.86.90.80'
    user_name = 'pylogin'
    pwd = 'Colgate#hp1'
    conn80 = ps.connect(host=host80, port='1433', user=user_name, password=pwd, database='Runtime')
    return conn80

def connection130():
    host130 = '10.86.90.130'
    # user_name = 'DataMonitoring'
    # pwd = 'Colgate01'
    user_name = 'sa'
    pwd = 'Colgate1'
    conn130 = ps.connect(host=host130, port='1433', user=user_name, password=pwd, database='Runtime')
    return conn130

def connection_ct():
    server_name = '10.86.181.70'
    user_name = 'fmcs'
    pwd = 'Colgate01'
    db = ps.connect(host=server_name, port='1433', user=user_name, password=pwd, database='ControlTower', charset='utf8')
    return db

def connection_alarm():
    server_name = '10.86.181.70'
    user_name = 'fmcs'
    pwd = 'Colgate01'
    db = ps.connect(host=server_name, port='1433', user=user_name, password=pwd, database='WWALMDB', charset='utf8')
    return db

def controltowereng():
    db = connection_ct()
    cursor = db.cursor()
    # tag_name1 = 'CT001_R_TempAI18'
    tag_name = []
    act_value = []
    cursor.execute("select name, value from dbo.tag WHERE name like '%_Energy'")
    row = cursor.fetchone()
    while row:
        tag_name.append(row[0])
        act_value.append(row[1])
        row = cursor.fetchone()
    data = pd.DataFrame({'Tag_Name': tag_name, 'Value': act_value})
    db.close()
    return data

def controltowerair():
    db = connection_ct()
    cursor = db.cursor()
    # tag_name1 = 'CT001_R_TempAI18'
    tag_name=[]
    act_value=[]
    cursor.execute("select name, value from dbo.tag WHERE name like 'CT009_N%'")
    row = cursor.fetchone()
    while row:
        tag_name.append(row[0])
        act_value.append(row[1])
        row = cursor.fetchone()
    tag_name.append('update_time')
    act_value.append(str(datetime.now()))
    data = pd.DataFrame({'Tag_Name': tag_name, 'Value': act_value})
    db.close()
    return data

def controltowersteam():
    db = connection_ct()
    cursor = db.cursor()
    tag_name=[]
    act_value=[]
    cursor.execute("select name, value from dbo.tag WHERE name like 'CT008_ZQ_Flow%' "
                   "or name like 'CT008_Water_Flow%' "
                   "or name like 'CT009_N20_DATA%'")
    row = cursor.fetchone()
    while row:
        tag_name.append(row[0])
        #act_value.append(float(row[1]))
        act_value.append(row[1])
        row = cursor.fetchone()
    tag_name.append('update_time')
    act_value.append(str(datetime.now()))
    data = pd.DataFrame({'Tag_Name': tag_name, 'Value': act_value})
    db.close()
    return data

def controltowerall():
    db = connection_ct()
    cursor = db.cursor()
    tag_name = []
    act_value = []
    tag_name.append('update_time')
    act_value.append(str(datetime.now()))
    cursor.execute("select name, value from dbo.tag WHERE name like 'GLJ%' "
                   "or name like 'CT%' "
                   "or name like 'CWED%'")
    row = cursor.fetchone()
    while row:
        tag_name.append(row[0])
        #act_value.append(float(row[1]))
        act_value.append(row[1])
        row = cursor.fetchone()
    data = pd.DataFrame({'Tag_Name': tag_name, 'Value': act_value})
    db.close()
    return data

def wwalmdbdata():
    db = connection_alarm()
    cursor = db.cursor()
    cursor.execute('select top 1000 * from v_AlarmEventHistory')
    a=[]
    b=[]
    c=[]
    d=[]
    e=[]
    f=[]
    g=[]
    h=[]
    i=[]
    j=[]
    k=[]
    l=[]
    m=[]
    n=[]
    o=[]
    p=[]
    q=[]
    r=[]
    s=[]
    t=[]
    u=[]
    row = cursor.fetchone()
    while row:
        if row[4] == 'ACSWD':
            row = cursor.fetchone()
        elif row[4] == '$System':
            row = cursor.fetchone()
        else:
            a.append(row[0])
            b.append(row[1])
            c.append(row[2])
            d.append(row[3])
            e.append(row[4])
            f.append(row[5])
            g.append(row[6])
            h.append(row[7])
            i.append(row[8])
            j.append(row[9])
            k.append(row[10])
            l.append(row[11])
            m.append(row[12])
            n.append(row[13])
            o.append(row[14])
            p.append(row[15])
            q.append(row[16])
            r.append(row[17])
            s.append(row[18])
            t.append(row[19])
            u.append(row[20])
            row = cursor.fetchone()
    data = pd.DataFrame({'EventStamp': a, 'AlarmState': b,  'TagName': c, 'Description':d, 'Area':e, 'Type':f, 'Value':g, 'CheckValue': h, 'Priority': i, 'Category':j, 'Provider':k,'Operator':l,'DomainName':m,'UserFullName':n,'UnAckDuration':o,'User1':p,'User2':q,'User3':r,'EventStampUTC':s,'MilliSec':t,'OperatorNode':u })
    data['EventStamp'] = data['EventStamp'].astype(str)
    data['EventStampUTC'] = data['EventStampUTC'].astype(str)
    print('Alarm Data is printed!!!!--- wwalmdbdata()')
    db.commit()
    db.close()
    return data

def get_data_google_sheets(sample_spreadsheet_id, tab_index):
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    # Read the .json file and authenticate with the links
    credentials = Credentials.from_service_account_file(
        'liquid-crossing-318502-21504d159a43.json',
        scopes=scopes
    )
    gc = gspread.authorize(credentials).open_by_key(sample_spreadsheet_id)
    values = gc.get_worksheet(tab_index).get_all_values()
    df = pd.DataFrame(values)
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    return df

def create_credentials():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = Credentials.from_service_account_file(
        'liquid-crossing-318502-21504d159a43.json',
        scopes=scopes
    )
    return gspread.authorize(credentials)

def read_spreadsheet(sample_spreadsheet_id, tab_index):
    gc = create_credentials()
    gc = gc.open_by_key(sample_spreadsheet_id)
    values = gc.get_worksheet(tab_index).get_all_values()
    df = pd.DataFrame(values)
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    return df

def update_spreadsheet(id_sheet, id_spreadsheet, df):
    gc = create_credentials()
    gc = gc.open_by_key(id_spreadsheet)
    sheet = gc.get_worksheet(id_sheet)
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    #print(sheet)
    #array = np.array([[1, 2, 3], [4, 5, 6]])
    #sheet.update('A6', array.tolist())

def select_max_data(conn, table):
    # global start_time, end_time
    end_time = datetime.now() - timedelta(hours=1)
    start_time = (end_time - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    df_grade = read_spreadsheet('1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE', table)
    tag_name = '\'' + df_grade['TagName'] + '\''
    tag_name = ','.join(tag_name.tolist())
    statement = "SET NOCOUNT ON \
                DECLARE @StartDate DateTime \
                DECLARE @EndDate DateTime \
                SET @StartDate = \'" + start_time + "\' \
                SET @EndDate = \'" + end_time + "\' \
                SET NOCOUNT OFF \
                SELECT  * FROM ( \
                SELECT History.TagName, Value, StartDateTime\
                FROM History \
                WHERE History.TagName IN (" + tag_name + ") \
                AND wwRetrievalMode = 'Max' \
                AND wwCycleCount = 1 \
                AND wwVersion = 'Latest' \
                AND Value is not null \
                AND DateTime >= @StartDate \
                AND DateTime <= @EndDate) temp \
                WHERE temp.StartDateTime >= @StartDate"
    df_sql = pd.read_sql(statement, con=conn)
    df_sql['StartDateTime'] = df_sql['StartDateTime'].astype(str)
    df_sql = df_sql.append({'TagName': 'refresh_time', 'Value': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                            'StartDateTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}, ignore_index=True)
    return df_sql

def select_min_data(conn, table):
    # global start_time, end_time
    end_time = datetime.now() - timedelta(hours=1)
    start_time = (end_time - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    df_grade = read_spreadsheet('1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE', table)
    tag_name = '\'' + df_grade['TagName'] + '\''
    tag_name = ','.join(tag_name.tolist())
    statement = "SET NOCOUNT ON \
                DECLARE @StartDate DateTime \
                DECLARE @EndDate DateTime \
                SET @StartDate = \'" + start_time + "\' \
                SET @EndDate = \'" + end_time + "\' \
                SET NOCOUNT OFF \
                SELECT  * FROM ( \
                SELECT History.TagName, Value, StartDateTime\
                FROM History \
                WHERE History.TagName IN (" + tag_name + ") \
                AND wwRetrievalMode = 'Min' \
                AND wwCycleCount = 1 \
                AND wwVersion = 'Latest' \
                AND Value is not null \
                AND DateTime >= @StartDate \
                AND DateTime <= @EndDate) temp \
                WHERE temp.StartDateTime >= @StartDate"
    df_sql = pd.read_sql(statement, con=conn)
    df_sql['StartDateTime'] = df_sql['StartDateTime'].astype(str)
    df_sql = df_sql.append({'TagName': 'refresh_time', 'Value': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                            'StartDateTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}, ignore_index=True)
    return df_sql

def select_average_data(conn, table):
    # global start_time, end_time
    end_time = datetime.now() - timedelta(hours=1)
    start_time = (end_time - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    df_grade = read_spreadsheet('1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE', table)
    tag_name = '\'' + df_grade['TagName'] + '\''
    tag_name = ','.join(tag_name.tolist())
    statement = "SET NOCOUNT ON \
                DECLARE @StartDate DateTime \
                DECLARE @EndDate DateTime \
                SET @StartDate = \'" + start_time + "\' \
                SET @EndDate = \'" + end_time + "\' \
                SET NOCOUNT OFF \
                SELECT  * FROM ( \
                SELECT History.TagName, Value, StartDateTime\
                FROM History \
                WHERE History.TagName IN (" + tag_name + ") \
                AND wwRetrievalMode = 'Average' \
                AND wwCycleCount = 1 \
                AND wwVersion = 'Latest' \
                AND Value is not null \
                AND DateTime >= @StartDate \
                AND DateTime <= @EndDate) temp \
                WHERE temp.StartDateTime >= @StartDate"
    df_sql = pd.read_sql(statement, con=conn)
    df_sql['StartDateTime'] = df_sql['StartDateTime'].astype(str)
    df_sql = df_sql.append({'TagName': 'refresh_time', 'Value': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                            'StartDateTime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}, ignore_index=True)
    return df_sql

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def update2sheet():

    #data_2 = wwalmdbdata()
    #update_spreadsheet(4, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data_2)
    #print('control_tower_Alarm is writen to Google Sheet!!!')

    conn = connection80()
    table = 0
    df_sql10 = select_max_data(conn, table)
    df_sql11 = select_min_data(conn, table)
    df_sql12 = select_average_data(conn, table)
    #print('program1 for connection80() is done, read the data')

    conn = connection130()
    table = 1
    df_sql20 = select_max_data(conn, table)
    df_sql21 = select_min_data(conn, table)
    df_sql22 = select_average_data(conn, table)
    #print('program2 for connection130() is done, read the data')

    df_sql_total0 = pd.concat([df_sql10, df_sql20], ignore_index=True)
    df_sql_total1 = pd.concat([df_sql11, df_sql21], ignore_index=True)
    df_sql_total2 = pd.concat([df_sql12, df_sql22], ignore_index=True)
    #print('df_sql_total is created')

    update_spreadsheet(9, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", df_sql_total0)
    update_spreadsheet(10, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", df_sql_total1)
    update_spreadsheet(8, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", df_sql_total2)
    #print('Data is writen to Google Sheet')

    data = controltowerair()
    table = 16
    update_spreadsheet(table, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data)

    data = controltowersteam()
    table = 19
    update_spreadsheet(table, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data)

    data = controltowerall()
    table = 7
    update_spreadsheet(table, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data)
    update_spreadsheet(0, "1pNzbXMNBJY4CfVlxGOj_CausnRJjuksIeobb65obhIw", data)
    update_spreadsheet(0, "1CuhTBjkQEuZknt699blyqvjmksXJzkgGb2FxT24dmk4", data)
    print("连接 CNN-80/130/air/stream 服务器成功!---update2sheet()", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

def update3sheet():

    data_1 = read_spreadsheet("1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", 3)
    data_1_4 = controltowereng()
    data_1_4 = data_1_4.rename(columns={'Value': str(datetime.now())})
    data_1 = data_1.merge(data_1_4, on='Tag_Name', how='left')
    # print(data_1)
    update_spreadsheet(3, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data_1)

    data_2 = read_spreadsheet("1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", 2)
    data_2_4 = controltowerair()
    data_2_4 = data_2_4.rename(columns={'Value': str(datetime.now())})
    data_2 = data_2.merge(data_2_4, on='Tag_Name', how='left')
    # print(data_2)
    update_spreadsheet(2, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data_2)
    print("连接 CNN-Energy/Air 服务器成功!--- update3sheet()")


if __name__ == '__main__':

    data = controltowerair()
    table = 16
    update_spreadsheet(table, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data)

    data1 = controltowersteam()
    table = 19
    update_spreadsheet(table, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data1)

    data = controltowerall()
    table = 7
    update_spreadsheet(table, "1-GeSlZFRg7d7tkcUD0yvLHLAxvajox22GHjK_meA9xE", data)
    update_spreadsheet(0, "1pNzbXMNBJY4CfVlxGOj_CausnRJjuksIeobb65obhIw", data)

    update2sheet()
    
    schedule.every(15).minutes.do(run_threaded, update2sheet)
    #schedule.every(1).day.at('00:00').do(run_threaded, update3sheet)
    #schedule.every(1).day.at('08:00').do(run_threaded, update3sheet)
    #schedule.every(1).day.at('16:00').do(run_threaded, update3sheet)

    print('main program is done!!!')

    while 1:
        schedule.run_pending()
        time.sleep(59)

