import requests
from bs4 import BeautifulSoup
import re
from mosprivoz import *
import sqlalchemy
import pandas as pd
import os
import psycopg2
import time
from datetime import time, datetime, date

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/95.0.4638.69 Safari/537.36',
    'accept': '*/*',
    'cookie':'_ga_K6FJY61J0S=GS1.1.1652784727.2.1.1652791209.0; _ym_visorc=w; _ga=GA1.1.840519978.1652778376; BITRIX_SM_GUEST_ID=61338; BITRIX_SM_LAST_VISIT=17.05.2022%2015%3A39%3A59; BITRIX_SM_SELECTED_CITY_CODE=0000073738; BITRIX_SM_SALE_UID=93064; _ym_d=1652778376; _ym_isad=2; _ym_uid=165277837650638319; BITRIX_CONVERSION_CONTEXT_s1=%7B%22ID%22%3A35%2C%22EXPIRE%22%3A1652821140%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; BX_USER_ID=5ecde604001dcc29be54e42061a55f5c; StartModal=true; directCrm-session=%7B%22deviceGuid%22%3A%22028bcc6e-ec79-4790-bb2a-4271d0abda83%22%7D; mindboxDeviceUUID=028bcc6e-ec79-4790-bb2a-4271d0abda83'

}

def parsing_data():
    HOST = 'https://mosprivoz.ru/sitemap/'  # Ссылка основного каталога
    shema = 'https://mosprivoz.ru'  # формирование ссылок для генератора


    r = requests.get(HOST, headers=HEADERS)
    soup = BeautifulSoup(r.text, 'lxml')
    a=[f"{shema}{x.find('a')['href']}" for x in soup.find('div',class_='child cat_menu').find_all('li',itemprop='name')]
    link=set(a)
    work_link = sorted(filter(lambda x: x.count('/') == 5,link))
    items={}
    shema = 'https://mosprivoz.ru'
    for links in work_link:
        type_link = []
        x = 1
        link = links + f"?PAGEN_1={x}"
        #bot.send_message(message.from_user.id, link)
        while x<20:
            link = links + f"?PAGEN_1={x}"
            r = requests.get(link, headers=HEADERS)
            soup = BeautifulSoup(r.text, 'lxml')
            try:
                i=soup.find('div',id='ts-pager-content').find_all('div',class_='table_item item_ws')
            except AttributeError:
                break
            i=i+soup.find('div',id='ts-pager-content').find_all('div',class_="table_item item_ws last-in-line")
            #print(len(i))
            step=1
            for item in i:
                art = item.find('a',class_='compare_item')['element_id'][1:]
                name = item.find('span',itemprop='name').text
                price=item.find('div',class_='price_block count')
                try: old_price = price.find('span',class_='old').text
                except AttributeError: old_price = '0'
                try: new_price = price.find('span',class_='new').text
                except AttributeError:
                    try: new_price = price.find('span').text
                    except AttributeError:
                        new_price = '0'
                lin = item.find('a')['href']
                items[art] = (links,name, old_price, new_price,shema+lin)
                step+=1
            try:
                pagen1 = soup.find('div', class_='pagination').find_all('a')[-1:]
                if re.search(r'(?<=PAGEN_1\=)\d+',pagen1[0]['href']) != None:
                    x+=1
                else:
                    break
            except AttributeError:
                break
    return items

#DB_URI = "postgres://tjwycdcqmhotok:e957b5593222ea8af40fe594d639775daec4ed46c51e34819abc6c4a74debfa9@ec2-54-229-217-195.eu-west-1.compute.amazonaws.com:5432/d864flhgj9d9at"

def data_post_to_base(items):
    host = "ec2-54-229-217-195.eu-west-1.compute.amazonaws.com"
    database="d864flhgj9d9at"
    user = "tjwycdcqmhotok"
    port = "5432"
    password = "e957b5593222ea8af40fe594d639775daec4ed46c51e34819abc6c4a74debfa9"
    engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
    engine.connect()
    parsing_moment = datetime.now()  # Записываем момент времени обработки
    parsing_day = date.today()
    #items = parsing_data()
    df = pd.DataFrame.from_dict(items, orient='index')  # перевели в DataFrame
    df.columns = ['links', 'name', 'old_price', 'new_price', 'link']
    df = df.reset_index()
    df['parsing_moment'] = parsing_moment
    df['parsing_day'] = parsing_day
    df.to_sql(
        name='mos_privoz_operational_metrics',
        schema='public',
        con=engine,
        index=False,
        if_exists='replace')