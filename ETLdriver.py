
# coding: utf-8

import urllib.request, json 
import SQLLiteDatabase as dlsl
from datetime import datetime, timedelta
import threading
import time
import sqlite3
from sqlite3 import Error


# Thread class for county wose data insert
class LoadDataThread(threading.Thread):

    def __init__ (self,DB,county,county_data):
        threading.Thread.__init__(self)
        self.DB = DB
        self.county = county
        self.county_data = county_data
        print('In thread: '+ self.county)
        id = threading.get_ident()
        
    def run(self):
        NYCovid_uri = 'file:NYCovid_database?mode=memory&cache=shared'
        self.conn = sqlite3.connect(NYCovid_uri, uri=True, check_same_thread=False, timeout=15)
        
    def join(self):
        self.tableName = self.DB.driver_dataLoad(self.conn,self.county,self.county_data)
        return(self.tableName)        

# function to read API and save data
def read_url():
    with urllib.request.urlopen("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD") as url:
        data = json.loads(url.read().decode())
    return(data)

# function to create connection
def connDB():
    DB = dlsl.SQLLiteDatabase()
    conn = DB.create_conn_sqlLite()
    return(DB,conn)

#function to load data in Database using thread
def loadData(DB,conn,data,isHistoric):
    tableNameList = []
    threadList = []
    today_date = datetime.today()
    yesterday_date_midnight = (today_date - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    county = ''
    county_data = []
    county_dict = {}
    
    for i in data.get('data'):
        
        if isHistoric:
            county_data.append([i[8],i[10],i[11],i[12],i[13],today_date.strftime('%Y-%m-%d %H:%M:%S')])
            if county == '':
                county = ''.join(x for x in i[9] if x.isalpha())
            elif county != '':
                if county != ''.join(x for x in i[9] if x.isalpha()):
                    county_dict[county] = county_data
                    county = ''.join(x for x in i[9] if x.isalpha())
                    county_data = []            
        else:        
            if (datetime.strptime(i[8], '%Y-%m-%dT%H:%M:%S') == yesterday_date_midnight):
                county = ''.join(x for x in i[9] if x.isalpha())
                county_data.append([i[8],i[10],i[11],i[12],i[13],today_date.strftime('%Y-%m-%d %H:%M:%S')])
                county_dict[county] = county_data
                county_data = []
                
    
    for key in county_dict.keys():
        print('---------------------------')
        print('Data inset for :' + key)
        thread = LoadDataThread(DB,key,county_dict.get(key))
        thread.start()
        threadList.append(thread)
    
    for thread in threadList:
        tableName = thread.join()
        tableNameList.append([tableName])
    
    return(tableNameList)

#function to select data
def selectData(DB,conn,tableNameList):
    DB.selectAllData(conn,tableNameList)
    
#function to close DB connection 
def closeDB(DB,conn):
    DB.close_conn_sqlLite(conn)
    
#main driver function for ETL process 
def main(isHistoric):
    data = read_url()
    DB,conn = connDB()
    tableNameList = loadData(DB,conn,data,isHistoric)
    selectData(DB,conn,tableNameList)
    closeDB(DB,conn)

# Starting main method
if __name__ == "__main__":
    main(isHistoric)