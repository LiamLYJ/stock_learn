import json
import sqlite3
import bs4
import urllib
from bs4 import BeautifulSoup
import ssl
import requests

from seleniumwire import webdriver
import urllib
import re

url = 'http://quotes.money.163.com/old/#query=EQA&DataType=HS_RANK&sort=PERCENT&order=desc&count=24&page=1'
driver = webdriver.Chrome()
driver.get(url)

# Access requests via the `requests` attribute
for request in driver.requests:
    if request.response and 'text/plain' in request.response.headers['Content-Type']:
        print ('xxxxxxxxxxxxxxxx')
        print(
            'path:: ', request.path, '\n',
            # request.response.status_code,
            'headers: ',request.response.headers['Content-Type'], '\n'
        )
        # tmp = request.response.headers['Content-Type']
        # print ('type of this: ', type(tmp))
        print ('xxxxxxxxxxxxxxxx')

driver.close()
# url  = 'http://quotes.money.163.com/hs/service/diyrank.php?host=http%3A%2F%2Fquotes.money.163.com%2Fhs%2Fservice%2Fdiyrank.php&page=1&query=STYPE%3AEQA&fields=NO%2CSYMBOL%2CNAME%2CPRICE%2CPERCENT%2CUPDOWN%2CFIVE_MINUTE%2COPEN%2CYESTCLOSE%2CHIGH%2CLOW%2CVOLUME%2CTURNOVER%2CHS%2CLB%2CWB%2CZF%2CPE%2CMCAP%2CTCAP%2CMFSUM%2CMFRATIO.MFRATIO2%2CMFRATIO.MFRATIO10%2CSNAME%2CCODE%2CANNOUNMT%2CUVSNEWS&sort=PERCENT&order=desc&count=24&type=query'
#
# html = urllib.request.urlopen(url).read()
# tmp = re.findall('\"CODE\".+?\"\d(\d+)\"', html.decode())
# print(tmp)

raise






url = 'http://quotes.money.163.com/old/#query=EQA&DataType=HS_RANK&sort=PERCENT&order=desc&count=24&page=2'

req = urllib.request.urlopen(url)
# r = requests.get(url)
# req = urllib.request.Request(url)
print('headers:', req.getheaders())
# print (r)
raise

conn = sqlite3.connect('stock.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Name;
DROP TABLE IF EXISTS Date;
DROP TABLE IF EXISTS Data;

CREATE TABLE Name (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name TEXT UNIQUE
);

CREATE TABLE Date (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    date TEXT UNIQUE
);

CREATE TABLE Data(
    name_id INTEGER,
    date_id INTEGER,
    open_price real,
    max_price real,
    min_price real,
    close_price real,
    price_range real,
    total_volume real,
    total_money real
)
''')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

key = '603088'
url = 'http://quotes.money.163.com/trade/lsjysj_603088.html?year=2018&season=1%27'

raw_data = urllib.request.urlopen(url, context=ctx).read()
soup = BeautifulSoup(raw_data, 'lxml')
parse_list = soup.select("div.inner_box tr")
for count, item in enumerate(parse_list[1:]):
    data = [x.string for x in item.select("td")]
    price = {
        "cur_timer": data[0],
        "cur_open_price": data[1],
        "cur_max_price": data[2],
        "cur_min_price": data[3],
        "cur_close_price": data[4],
        "cur_price_range": data[6],
        "cur_total_volume": data[7],
        "cur_total_money": data[8]
    }
    print ('price', price)

    cur.execute('''INSERT OR IGNORE INTO Name (name)
        VALUES ( ? )''', ( key, ) )
    cur.execute('SELECT id FROM Name WHERE name = ? ', (key, ))
    name_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO Date (date)
        VALUES ( ? )''', ( data[0], ) )
    cur.execute('SELECT id FROM Date WHERE date = ? ', (data[0], ))
    date_id = cur.fetchone()[0]


    cur.execute('''INSERT INTO Data
                (
                    name_id ,
                    date_id ,
                    open_price ,
                    max_price ,
                    min_price ,
                    close_price ,
                    price_range ,
                    total_volume ,
                    total_money
                ) VALUES (?,?,?,?,?,?,?,?,?)''',
                (
                    name_id, date_id,
                    data[1],
                    data[2],
                    data[3],
                    data[4],
                    data[6],
                    data[7],
                    data[8]
                )
                )

    if count % 10 == 0:
        conn.commit()
