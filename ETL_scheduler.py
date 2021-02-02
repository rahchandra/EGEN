# coding: utf-8

import schedule
import time
import ETLdriver as ETL

def job(isHistoric):
    ETL.main(isHistoric)


isHistoric = True
schedule.every(10).seconds.do(job,isHistoric)
# schedule.every().day.at("09:00").do(job,isHistoric)

while True:
    schedule.run_pending()
    time.sleep(10)
