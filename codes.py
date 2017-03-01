import requests
import sqlite3

url='https://www.mastercard.us/settlement/currencyrate/settlement-currencies'
r = requests.get(url)
JSON=r.json()

codes=list()
for currency in JSON['data']['currencies']:
    codes.append((currency['alphaCd'].strip(),currency['currNam'].strip()))
codes=sorted(codes)

conn = sqlite3.connect('mastercard_1.sqlite')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS Currency_Codes')
cur.execute('CREATE TABLE Currency_Codes (id  INTEGER PRIMARY KEY, currency TEXT UNIQUE,code TEXT UNIQUE)')

for (code, currency) in codes:
    cur.execute('''INSERT OR IGNORE INTO Currency_Codes (currency, code) 
    VALUES ( ?, ? )''', (currency, code ) )
conn.commit()

