import sqlite3
conn = sqlite3.connect('mastercard_db.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Rates;

CREATE TABLE Rates (
    id  INTEGER PRIMARY KEY,
    rate REAL,
    from_id INTEGER,
    to_id INTEGER,
    date_id INTEGER
     
);
''')