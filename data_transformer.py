import os
import json
import boto3
import random
import datetime
import yfinance as yf
from time import sleep


kinesis = boto3.client('kinesis', "us-east-1")
def lambda_handler(event, context):
    stocks=['MSFT','FB','SHOP','BYND','NFLX','PINS','SQ','TTD','OKTA','SNAP','DDOG']
    data = {}
    for i in stocks:
        name = yf.Ticker(i)
        hist = name.history(start="2022-05-02",end = '2022-05-03', period='1d', interval = '5m')
        hist = hist.loc['2022-05-02']
        for index, row in hist.iterrows():
            data = {'high':row['High'], 'low':row['Low'], 'ts':index.strftime('%Y-%m-%d %X'), 'name': i}
            data2 = json.dumps(data)+"\n"
            print(json)
            kinesis.put_record(
                        StreamName=os.environ['Streamname'],
                        Data=data2,
                        PartitionKey="partitionkey")
            sleep(1)
