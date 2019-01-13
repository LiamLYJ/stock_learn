import bs4
import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import ssl
from sqlite_db import data_base

class data_crawler:
    def __init__(self, save_db_name):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.ctx = ctx
        self.stock_db =  data_base(save_db_name)

    def get_data(self, year, season, key):
        url = "http://quotes.money.163.com/trade/lsjysj_" + key + ".html?year=" + year + "&season=" + season
        raw_data = urllib.request.urlopen(url, context=self.ctx).read()
        soup = BeautifulSoup(raw_data, 'lxml')
        parse_list = soup.select("div.inner_box tr")
        self.stock_db.add_key(key)
        for count, item in enumerate(parse_list[1:]):
            data = [x.string for x in item.select("td")]
            filter_data = {
                "cur_timer": data[0],
                "cur_open_price": data[1],
                "cur_max_price": data[2],
                "cur_min_price": data[3],
                "cur_close_price": data[4],
                "cur_price_range": data[6],
                "cur_total_volume": data[7],
                "cur_total_money": data[8]
            }
            print ('filter_data', filter_data)
            self.stock_db.add_data(filter_data, key)

            if count % 10 == 0:
                self.stock_db.commit()
