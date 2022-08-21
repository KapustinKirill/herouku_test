#Импортируем библиотеки:
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import string
import sqlalchemy
from datetime import datetime, date


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/95.0.4638.69 Safari/537.36',
    'accept': '*/*',
    'cookie':'_ga_K6FJY61J0S=GS1.1.1652784727.2.1.1652791209.0; _ym_visorc=w; _ga=GA1.1.840519978.1652778376; BITRIX_SM_GUEST_ID=61338; BITRIX_SM_LAST_VISIT=17.05.2022%2015%3A39%3A59; BITRIX_SM_SELECTED_CITY_CODE=0000073738; BITRIX_SM_SALE_UID=93064; _ym_d=1652778376; _ym_isad=2; _ym_uid=165277837650638319; BITRIX_CONVERSION_CONTEXT_s1=%7B%22ID%22%3A35%2C%22EXPIRE%22%3A1652821140%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; BX_USER_ID=5ecde604001dcc29be54e42061a55f5c; StartModal=true; directCrm-session=%7B%22deviceGuid%22%3A%22028bcc6e-ec79-4790-bb2a-4271d0abda83%22%7D; mindboxDeviceUUID=028bcc6e-ec79-4790-bb2a-4271d0abda83'

}  # для отправки заголовков, чтобы сервер не посчитал нас ботами и не забанил




# Собираем все категории сайта:
async def gather_data1():
    tasks = []
    async with aiohttp.ClientSession() as session:
        response = await session.get(url=HOST, headers=HEADERS)
        soup = BeautifulSoup(await response.text(), "lxml")
        a = [f"{shema}{x.find('a')['href']}" for x in soup.find_all('div', class_='menu__item')]
        link = set(a)
        work_link = sorted(filter(lambda x: x.count('/') == 6, link))
        for link in work_link:

            task = asyncio.create_task(get_page_data1(session,link))
            tasks.append(task)
        await asyncio.gather(*tasks)

# Переменные для товаров

# Собираем все ссылки на товары сайта

async def get_page_data1(session,path):
    nonlocal count_item
    nonlocal d
    nonlocal d1
    async with session.get(url=path, headers=HEADERS) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, 'lxml')
        page_item=int(''.join(filter(lambda x: x in string.digits,soup.find('span',class_="section__title-info").text.strip())))
        count_item += page_item
        product1=[ x.find('h4', class_= "product__title").text for x in soup.find_all('div',class_="product__info-left")]
        link_item=[f"{shema}{x['href']}" for x in soup.find_all('a',class_="product__link")]
        d = d + product1
        d1 = d1 + link_item
        if page_item> 15:
            print(page_item)
            for step_page in range(2,page_item//15+1 if page_item%15==0 else page_item//15+2):
                param={f'PAGEN_1':step_page}
                async with session.get(url=path, headers=HEADERS, params=param) as response:
                    response_text = await response.text()
                    #r = requests.get(item, headers=HEADERS, params=param)
                    soup = BeautifulSoup(response_text, 'lxml')
                    page_item=int(''.join(filter(lambda x: x in string.digits,soup.find('span',class_="section__title-info").text.strip())))
                    #count_item += page_item
                    product1=[ x.find('h4', class_= "product__title").text for x in soup.find_all('div',class_="product__info-left")]
                    link_item=[f"{shema}{x['href']}" for x in soup.find_all('a',class_="product__link")]
                    d = d + product1
                    d1 = d1 + link_item


async def gather_data():
    tasks = []
    async with aiohttp.ClientSession() as session:
        for link in unique_link:
            task = asyncio.create_task(get_page_data(session,link))
            tasks.append(task)
        await asyncio.gather(*tasks)

#Обходим все товары на сайте




async def get_page_data(session,path):
    nonlocal items
    async with session.get(url=path, headers=HEADERS) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, "lxml")
        if len(items)%50 == 0: print(len(items))
        try:
            text=soup.find('div',class_="prod-detail__cost cost cost--big").find('p',class_="cost__val").text.strip()
            price_item=int(''.join(filter(lambda x: x in string.digits,text)))
            article_item=path
            name_item=soup.find('h1',class_="prod-detail__big-title").text.strip()
            items[article_item] = (name_item, price_item, True)
        except AttributeError:
            article_item=path
            name_item=soup.find('h1',class_="prod-detail__big-title").text.strip()
            items[article_item] = (name_item, None, False)

def parsing_vse_smart(bot,message):
    HOST = 'https://www.vsesmart.ru/catalog/'  # Ссылка основного каталога
    shema = 'https://www.vsesmart.ru'  # формирование ссылок для генератора
    items = {}
    count_step = 0
    d = []
    d1 = []
    count_item = 0
    asyncio.run(gather_data1())
    unique_link=set(d1)
    bot.send_message(message.from_user.id, "товары собраны',len(unique_link)")
    asyncio.run(gather_data())
    df=pd.DataFrame.from_dict(items,orient='index')# перевели в DataFrame
    df.columns=['name','price','key']

    # ip базы куда запысываем - так как он динамический приходится менять

    parsing_moment = datetime.now()  # Записываем момент времени обработки
    parsing_day = date.today()  # День обработки
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
    df = df.reset_index()
    df['parsing_moment'] = parsing_moment
    df['parsing_day'] = parsing_day
    df.to_sql(
        name='vse_smart_operational_metrics',
        schema='public',
        con=engine,
        index=False,
        if_exists='replace')