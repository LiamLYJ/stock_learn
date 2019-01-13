from crawler import data_crawler
from sqlite_db import data_base


# url = 'http://quotes.money.163.com/trade/lsjysj_603088.html?year=2018&season=1%27'
tmp = data_crawler('xxx.sqlite')
tmp.get_data('2018','1', '603088')
