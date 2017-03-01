import datetime
import sqlite3
import pytz

conn = sqlite3.connect('mastercard_1.sqlite')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS Dates')
cur.execute('CREATE TABLE Dates (id  INTEGER PRIMARY KEY, date TEXT UNIQUE)')

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
cur.execute('''INSERT OR IGNORE INTO Dates (date) 
VALUES ( ? )''', ( date_1, ))
for i in range(1000):
    date = date_1+datetime.timedelta(days=i)
    cur.execute('''INSERT OR IGNORE INTO Dates (date) 
    VALUES ( ? )''', ( date, ))
conn.commit()