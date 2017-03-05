# 151 currencies, 22650 currency pairs, 364 days (period 1) 134 days (period 2) => 3,035,100(20,100/from_c) - 8,221,950 entries(8361.157)

#####IMPORT PACKAGES

print('importing packages')
import time
import sqlite3
import json
import requests
import datetime
import pytz
from datetime import date
from multiprocessing.pool import Pool

###### Create/connect to database (sqlite file)

print('connecting to db')
conn = sqlite3.connect('mastercard_db.sqlite')
cur = conn.cursor()

###### Defining Functions, Convert dates into different types, date/day/string, chunkIt splits currency codes into approximately equal chunks

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
    
###### Defining constants, first_date is first_date in database, now is datetime in EST, today is last date rates available for, date_1 is first date rates available for
    
print('defining constants')
start_from_id = int(input('from_id initial value: '))
start_to_ids= [int(x) for x in input('List of to_ids, seperated by spaces').split()]
n = len(start_to_ids)

base_url = 'https://www.mastercard.us/settlement/currencyrate/fxDate={date_};transCurr={from_};crdhldBillCurr={to_};bankFee=0.00;transAmt=1/conversion-rate'

first_date=date(2016,2,29)
now = datetime.datetime.now(pytz.timezone('US/Eastern'))
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

######### Extracts all exchnage rates from 14th Oct onward

def extract_rates(from_id,to_id):
    if to_id>151:
        entry='done'
        entries.append(entry)
        return entries
    if from_id is 'done':
        entry='done'
        entries.append(entry)
        return entries   
    
    ######### Creates URL
    
    else:
        from_c = codes[from_id-1]
        to_c = codes[to_id-1]
        print(from_c,to_c)
        day=late_day
        date=date_calculator(day) 
        print('extracting rates...')
        while (today - date).days >=0:
            date_string=date_stringer(date)
            url=base_url.format(date_=date_string,from_=from_c,to_=to_c)

            #Retries if requests doesn't return a json file (server errors)
            while True:
                try:
                    r = requests.get(url)
                    JSON=r.json()
                except:
                    time.sleep(5) 
                    continue
                break
            ######### Error Handling
            
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
            
            ######### Adds conversion rate with date_id and currency ids
            
            else:
                rate = JSON['data']['conversionRate']
                day = day_calculator(date)     
                date_id=(date_1-first_date).days+day
                entry=(rate,from_id,to_id,date_id)
                entries.append(entry)
                day+=1
                date=date_calculator(day)
        
        return entries    



print('initiating')
entries=list()
chunks=chunkIt(range(start_from_id,152),n)
for code in codes[(start_from_id-1):]:
    try:
        to_ids
    except:
        to_ids = start_to_ids
        from_ids = [chunks[x][0] for x in range(0,n)]
        last_from_ids = [chunks[x][-1] for x in range(0,n)]
    while any(from_ids[x] != 'done' for x in range(0,n)):
        while all(to_id <=151 for to_id in to_ids):
            entries.clear()
            for i in range (0,n): 
                if from_ids[i] is to_ids[i]:
                        to_ids[i] +=1
                        continue
            for i in range (0,n):        
                print(from_ids[i],to_ids[i])

            start_time = datetime.datetime.now()     
            ### Multithread with n threads
            p = Pool(processes=n)
            ### Returns list of n lists of entries for the year for n currency codes
            entries_list =p.starmap(extract_rates, [(from_ids[x],to_ids[x]) for x in range(0,n) ])
            p.close()
            for entries in entries_list:
                for entry in entries:
                    if entry == 'done':
                        pass
                    else:
                        cur.execute('''INSERT OR REPLACE INTO Rates
                        (rate, from_id, to_id, date_id)
                        VALUES ( ?, ?, ?, ?)''', 
                        (entry[0], entry[1], entry[2], entry[3]) )
                conn.commit()
                end_time = datetime.datetime.now()
                print('Duration: {}'.format(end_time - start_time))
            to_ids[:] = [x+1 for x in to_ids]
            
            ### Updates current date to ensure that if time has passed still collecting data for all available dates
            
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


        from_ids[:] = ['done' if from_ids[x] in ('done',last_from_ids[x]) else from_ids[x]+1  for x in range(0,n)]
        print (from_ids)
        to_ids[:] = [1] * n
  

