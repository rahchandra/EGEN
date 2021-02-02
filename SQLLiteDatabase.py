
# coding: utf-8

import sqlite3
from sqlite3 import Error


class SQLLiteDatabase:

    #private function to create connection
    def _create_connection(self):
        conn = None;
        try:
            NYCovid_uri = 'file:NYCovid_database?mode=memory&cache=shared'
            conn = sqlite3.connect(NYCovid_uri, uri=True, check_same_thread=False)
            print(sqlite3.version)
            print(conn)
            return (conn)
        except Error as e:
            print(e)

    #private function to create table
    def _create_table(self,conn,county):
        try:
            tableName = "NY_"+county+"_data"
            sql_create__table = """ CREATE TABLE IF NOT EXISTS """+tableName+""" (
                                                testDate text,
                                                newPositive integer,
                                                cumulativeNumberofPositives integer,
                                                totalNumberofTestsPerformed integer,
                                                cumulativeNumberofTestsPerformed integer,
                                                loadDate text
                                            ); """
            c = conn.cursor()
            c.execute(sql_create__table)
            conn.commit()
            c.close()
            return (tableName)
        except Error as e:
            return(e)

    #private function to insert data
    def _insert_date(self, conn, tableName, county_data):
        try:
            for i in county_data:
                sql = ''' INSERT INTO '''+tableName+'''(testDate, newPositive, cumulativeNumberofPositives, totalNumberofTestsPerformed, cumulativeNumberofTestsPerformed, loadDate)
                      VALUES(?,?,?,?,?,?) '''
                cur = conn.cursor()
                cur.execute(sql, i)
                conn.commit()
                id = cur.lastrowid
                cur.close()
        except Exception as e:
            print("Error in data insert :" + str(e))
            

    #private function to select data
    def _select_data(self,conn, tableName):
        try:   
            
            cur = conn.cursor()
            print(tableName[0])
            cur.execute("SELECT * FROM "+tableName[0])

            rows = cur.fetchall()
            for row in rows:
                print(row)

            cur.close()
        except Exception as e:
            print("Error in select:" + str(e))
            
    #private function to close connection
    def _close_connection(self, conn):
        conn.close()

    #public function to initate table creation and data insertion
    def driver_dataLoad(self, conn, county, county_data):
        tableName = self._create_table(conn, county)
        self._insert_date(conn, tableName, county_data)
        return (tableName)

    #public function to initate SQLITE connection 
    def create_conn_sqlLite(self):
        conn = self._create_connection()
        return(conn)

    #public function to close connection 
    def close_conn_sqlLite(self, conn):
        self._close_connection(conn)

    #public function to select data
    def selectAllData(self, conn, tableNameList):
        for tableName in tableNameList:
            self._select_data(conn,tableName)