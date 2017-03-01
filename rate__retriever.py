# 151 currencies, 22650 currency pairs, 364 days (period 1) 134 days (period 2) => 3,035,100(20,100/from_c) - 8,221,950 entries(8361.157)


print('importing packages')
import time
import sqlite3
import json
import requests
import datetime
import math
import pytz
from datetime import date
from multiprocessing.pool import Pool

print('connecting to db')
conn = sqlite3.connect('mastercard_1.sqlite')
cur = conn.cursor()

print('defining functions')
def day_calculator(date):
    return (date - date_1).days + 1
def date_calculator(day):
    return date_1+datetime.timedelta(day-1)
def date_stringer(date):
    return date.strftime('%Y-%m-%d')

print('defining constants')
start_from_id = int(input('from_id initial value: '))
start_to_id = int(input('to_id initial value: '))
base_url = 'https://www.mastercard.us/settlement/currencyrate/fxDate={date_};transCurr={from_};crdhldBillCurr={to_};bankFee=0.00;transAmt=1/conversion-rate'

first_date=date(2016,2,29)
now = datetime.datetime.now(pytz.timezone('US/Eastern'))
if now.hour < 14:
    today=now.date() - datetime.timedelta(days=1)
else:
    today=now.date()
    
date_1=today - datetime.timedelta(days=364)
if date_1.weekday()==6:
    date_1=date_1+datetime.timedelta(days=1)
if date_1.weekday()==5:
    date_1=date_1+datetime.timedelta(days=2)

date_string = date_stringer(date_1)
print('first date in period', date_1, 'today:',today) 
late_day=day_calculator(date(2016,10,14))

print('grabbing codes from db')
cur.execute('SELECT code FROM Currency_Codes')
code_tuples=cur.fetchall()
codes = [ x[0] for x in code_tuples ]
number_of_codes = len(codes)




print('initiating')
for code in codes[(start_from_id-1):]:
    start_time_f = datetime.datetime.now()
    to_id = start_to_id
    from_c = code
    cur.execute('SELECT id FROM Currency_Codes WHERE code=?', (from_c,))
    from_id = cur.fetchone()[0]
    while to_id <= number_of_codes:
        start_time_t = datetime.datetime.now()
        to_c = codes[to_id-1]
        print(from_c,to_c)
        if from_c is to_c:
            to_id +=1
            continue
        
####   FIND START DATE - FIRST CHECKS LATE DAY, THEN FIRST DAY, THEN DOES BINARY SEARCH
        lower_bound=1
        upper_bound=late_day
        day_i=late_day-1
        while upper_bound != lower_bound:   
            date_i = date_calculator(day_i)
            if day_i < late_day-4:
                if date_i.weekday() == 6:
                    if lower_bound <= day_i-2 :
                        day_i=day_i-2
                if date_i.weekday() == 5:
                    if lower_bound <= day_i-1:
                        day_i=day_i-1    
            date_i = date_calculator(day_i)
            date_string_i=date_stringer(date_i)
            url=base_url.format(date_=date_string_i,from_=from_c,to_=to_c)
            print(date_string_i,'day number:', day_i,'day of the week:', date_i.weekday())

            #Retries if requests doesn't return a json file (server errors)
            print('requesting url')
            while True:
                try:
                    r = requests.get(url)
                    JSON=r.json()
                except:
                    time.sleep(5) 
                    continue
                break
    
            print('json retrieved')
            if 'errorCode' in JSON['data']:
                if JSON['data']['errorCode'] in ('104','114'):
                    print('data not available for this date')
                    lower_bound = day_i+1
                    if day_i==late_day-1:
                        day_i=late_day
                        break 
                    else:
                        day_i=math.ceil((lower_bound+upper_bound)/2)                                                  
                        print('lower:',lower_bound,'upper:',upper_bound)
                elif JSON['data']['errorCode'] in ('500','401','400'):
                    print('error code: ',JSON['data']['errorCode'])
                    print('Server having technical problems')
                    time.sleep(500)
                    continue
                
                else:
                    print('error code: ',JSON['data']['errorCode'])
                    print('conversion rate too small')
                    break
            else:
                upper_bound = day_i
                if day_i == late_day-1:
                    day_i=1
                elif day_i == 1:
                    break
                else:
                    day_i=math.floor((lower_bound+upper_bound)/2)    
                    print('lower:',lower_bound,'upper:',upper_bound)      

####   Extract rates for period up to today
        day=day_i
        date=date_calculator(day_i)
        print('found start day')  
        start_=datetime.datetime.now()
        while (today - date).days >=0:
            if day < late_day-4:
                if date.weekday() == 5:
                    day = day + 2
                    date = date_calculator(day)
            date_string=date_stringer(date)
            url=base_url.format(date_=date_string,from_=from_c,to_=to_c)
            print(date)

            #Retries if requests doesn't return a json file (server errors)
            print('requesting url')
            while True:
                try:
                    r = requests.get(url)
                    JSON=r.json()
                except:
                    time.sleep(5) 
                    continue
                break
        
            print('json retrieved')
            if 'errorCode' in JSON['data']:
                if JSON['data']['errorCode'] in ('104','114'):
                    print('data not available for this date')
                    day = day + 1
                    date = date_calculator(day)
                    continue
                elif JSON['data']['errorCode'] in ('500','401','400'):
                    print('error code: ',JSON['data']['errorCode'])
                    print('Server having technical problems')
                    time.sleep(500)
                    continue
                
                else:
                    print('error code: ',JSON['data']['errorCode'])
                    print('conversion rate too small')
                    break
            else:
                rate = JSON['data']['conversionRate']
                day = day_calculator(date)     
                print(rate)
                date_id=(date_1-first_date).days+day
                cur.execute('''INSERT OR REPLACE INTO Rates
                (rate, from_id, to_id, date_id)
                VALUES ( ?, ?, ?, ?)''', 
                (rate, from_id, to_id, date_id) ) 
                day = day + 1
                date = date_calculator(day)
        end_ = datetime.datetime.now()
        print(from_c,'Duration: {}'.format(end_ - start_))    
        to_id +=1
        conn.commit()
        end_time_t = datetime.datetime.now()
        print(to_c,'Duration: {}'.format(end_time_t - start_time_t))
        date_1=today - datetime.timedelta(days=364)
        if date_1.weekday()==6:
            date_1=date_1+datetime.timedelta(days=1)
        if date_1.weekday()==5:
            date_1=date_1+datetime.timedelta(days=2)
        now = datetime.datetime.now(pytz.timezone('US/Eastern'))
        if now.hour < 14:
            today=now.date() - datetime.timedelta(days=1)
        else:
            today=now.date()
    end_time_f = datetime.datetime.now()
    print(from_c,'Duration: {}'.format(end_time_f - start_time_f))
print('done')

