from crawler import data_crawler, name_crawler
from sqlite_db import data_base

# check base_url = 'http://quotes.money.163.com/old/#query=EQA&DataType=HS_RANK&sort=PERCENT&order=desc&count=24&page='
MAX_PAGE = 148

my_name_crawler = name_crawler()

def get_stock_ids():
    page = 0
    while True:
        if page > MAX_PAGE - 1:
            break
        yield my_name_crawler.get_names(page)
        page += 1

my_crawler = data_crawler('xxx.sqlite')

# one page of base_url contains n names
for names in get_stock_ids():
    for key in names:
        for year in range(2005, 2019, 1):
        # for year in range(2019, 2020, 1):
            for season in range(1,4,1):
            # for season in range(1,2,1):
                my_crawler.get_data(str(year), str(season), key)

del my_name_crawler
