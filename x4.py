import json
import sqlite3
import urllib.request, urllib.parse, urllib.error
import ssl
from datetime import datetime

conn = sqlite3.connect('xratedb.sqlite')
cur = conn.cursor()

# Database set up

# DROP TABLE IF EXISTS Date;
# DROP TABLE IF EXISTS Currency;
# DROP TABLE IF EXISTS Xrate;

cur.executescript('''

CREATE TABLE IF NOT EXISTS Date (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS Base (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS Currency (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS Xrate (
    currency_id INTEGER,
    date_id INTEGER,
    base_id INTEGER,
    value INTEGER,
    PRIMARY KEY(currency_id, base_id, date_id)

)
''')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


base = input('Choose base: ')
if ( len(base) < 1 ) : base = 'EUR'
symbols = input('Choose currency: ')
if ( len(symbols) < 1 ) : symbols = 'USD,GBP,SEK'
# start date is 2020-12-01 by default. Not great but if I pick up the latest date, it is the latest date regardless of the pair...
start = input('Start date YYYY-MM-DAY: ')
if ( len(start) < 1 ) : start = '2020-12-01'
# end date is today by default
end = input ('End date YYYY-MM-DAY:   ')
if ( len(end) < 1 ) :
    end = datetime.today().strftime('%Y-%m-%d')


serviceur = 'https://api.exchangeratesapi.io/history?'
url = serviceur + 'start_at=' + start + '&end_at=' + end + '&symbols=' + symbols + '&base=' + base

print ('Retrieving:',url)
uh = urllib.request.urlopen(url, context=ctx).read()
data = uh.decode()
info = json.loads(data)

cur.execute('''INSERT OR IGNORE INTO Base (name)
                 VALUES ( ? )''', ( base, ) )
cur.execute('SELECT id FROM Base WHERE name = ? ', (base, ))
base_id = cur.fetchone()[0]

# print (type(base_id))
# print (base_id)

dico=info['rates']
for date in dico:
    # print (date, dico[date], dico[date]['USD'])

    cur.execute('''INSERT OR IGNORE INTO Date (name)
                 VALUES ( ? )''', ( date, ) )
    cur.execute('SELECT id FROM Date WHERE name = ? ', (date, ))
    date_id = cur.fetchone()[0]
    # print ('date_id is ',date_id)

    for key,value in dico[date].items():
        cur.execute('''INSERT OR IGNORE INTO Currency (name)
                 VALUES ( ? )''', ( key, ) )
        cur.execute('SELECT id FROM Currency WHERE name = ? ', (key, ))
        currency_id = cur.fetchone()[0]
        # print ('currency_id is :',currency_id)

        cur.execute('''INSERT OR IGNORE INTO Xrate (date_id, base_id, currency_id, value)
                 VALUES ( ?,?,?,? )''',
                 ( date_id, base_id, currency_id, value  ) )
        # print ('value is :',value)

conn.commit()
