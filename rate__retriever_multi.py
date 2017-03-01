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
import sys

print('connecting to db')
conn = sqlite3.connect('mastercard_db.sqlite')
cur = conn.cursor()

print('defining functions')
def day_calculator(date):
    return (date - date_1).days + 1
def date_calculator(day):
    return date_1+datetime.timedelta(day-1)
def date_stringer(date):
    return date.strftime('%Y-%m-%d')
def chunkIt(seq, num):
  avg = len(seq) / float(num)
  out = []
  last = 0.0

  while last < len(seq):
    out.append(seq[int(last):int(last + avg)])
    last += avg

  return out
    

print('defining constants')
start_from_id = int(input('from_id initial value: '))
start_to_ids=[int(x) for x in input('numbers seperated by spaces ').split()]
number_of_threads=input('number of threads ')
n = int(number_of_threads)
base_url = 'https://www.mastercard.us/settlement/currencyrate/fxDate={date_};transCurr={from_};crdhldBillCurr={to_};bankFee=0.00;transAmt=1/conversion-rate'

first_date=date(2016,2,29)
now = datetime.datetime.now(pytz.timezone('US/Eastern'))
print('now: ', now)
if now.hour < 14:
    today=now.date() - datetime.timedelta(days=1)
else:
    today=now.date()
print('today: ', today)    
date_1=today - datetime.timedelta(days=364)
if date_1.weekday()==6:
    date_1=date_1+datetime.timedelta(days=1)
if date_1.weekday()==5:
    date_1=date_1+datetime.timedelta(days=2)
print(date_1)

date_string = date_stringer(date_1)
print('first date in period', date_1, 'today:',today) 
late_day=day_calculator(date(2016,10,14))

print('grabbing codes from db')
cur.execute('SELECT code FROM Currency_Codes')
code_tuples=cur.fetchall()
codes = [ x[0] for x in code_tuples ]
number_of_codes = len(codes)

####   FIND START DATE - FIRST CHECKS LATE DAY, THEN FIRST DAY, THEN DOES BINARY SEARCH
def find_start_day(from_c,to_c):
    if to_c=='done':
        return ('done',from_c,to_c)    
    else:
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
                elif JSON['data']['errorCode'] in ('500','400'):
                    print('error code: ',JSON['data']['errorCode'])
                    print('Server having technical problems')
                    time.sleep(500)
                    continue
                elif JSON['data']['errorCode'] in ('401'):
                    print('error code: ',JSON['data']['errorCode'])
                    print('data not available for this date')
                    lower_bound = day_i+1
                    day_i+=1
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
        print('found start day', lower_bound)      
        return (lower_bound,from_c,to_c)

def extract_rates(start_day,from_c,to_c):
    if start_day=='done':
        entry='done'
        entries.append(entry)
        return entries    
    else:
        day=start_day
        date=date_calculator(day) 
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
                entry=(rate,from_c,to_c,date_id)
                entries.append(entry)
                day+=1
                date=date_calculator(day)
        return entries    



print('initiating')
entries=list()
for code in codes[(start_from_id-1):]:
    chunks=chunkIt(range(start_from_id,151),n)
    try:
        to_ids
    except:
        to_ids = start_to_ids
        from_ids = [chunks[x][0] for x in range(0,n)]
        print(from_ids)
    print(to_ids)
    
    from_cs = [codes[from_ids[x]-1] for x in range(0,n)]
    print(from_cs)
    print('from set')
    while all(to_id != 'done' for to_id in to_ids):
        for i in range (0,n): 
            if from_ids[i] is to_ids[i]:
                    to_ids[i] +=1
                    continue
        to_cs = ['done' if to_ids[x] == 'done' else codes[to_ids[x]-1] for x in range(0,n)]
        print(to_cs)
        for i in range (0,n):        
            print(from_cs[i],to_cs[i])
        start_time = datetime.datetime.now()     
        p = Pool(processes=n)
        function_1 = find_start_day
        arguments_1 = [(from_cs[x],to_cs[x]) for x in range(0,n) ]

        start_days = p.starmap(function_1, arguments_1)
        
        function_2 = extract_rates
        arguments_2 = start_days
        
        entries_list =p.starmap(function_2, arguments_2)
        for entries in entries_list:
            for entry in entries:
                if entry == 'done':
                    pass
                else:
                    cur.execute('''INSERT OR REPLACE INTO Rates
                    (rate, from_id, to_id, date_id)
                    VALUES ( ?, ?, ?, ?)''', 
                    (entry[0], codes.index(entry[1])+1, codes.index(entry[2])+1, entry[3]) )
            conn.commit()
            end_time = datetime.datetime.now()
            print('Duration: {}'.format(end_time - start_time))
        for to_id in to_ids:
            if to_id == 151:
                to_id = 'done'
            if to_id < 151:
                to_id +=1
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
    for from_id in from_ids:
        from_id+=1
  

