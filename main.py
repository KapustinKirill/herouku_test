import os
import telebot
import logging
import psycopg2
from mosprivoz import *
from vse_smart import *
from flask import Flask, request


BOT_TOKEN = os.getenv('API_TOKEN')
APP_URL = "https://dhawktestapplication.herokuapp.com/" + BOT_TOKEN
DB_URI = os.getenv('DATABASE_URL')
bot = telebot.TeleBot(BOT_TOKEN)
server = Flask(__name__)
logger = telebot.logger
logger.setLevel(logging.DEBUG)

db_connection = psycopg2.connect(DB_URI, sslmode="require")
db_object = db_connection.cursor()

def update_messages_count(user_id):
    db_object.execute(f"UPDATE users SET messages = messages + 1 WHERE id = {user_id}")
    db_connection.commit()


@bot.message_handler(commands=["start"])
def start(message):
    user_id = message.from_user.id
    username = message.from_user.username
    bot.reply_to(message, f"Hello, {username}!")

    db_object.execute(f"SELECT id FROM users WHERE id = {user_id}")
    result = db_object.fetchone()

    if not result:
        db_object.execute("INSERT INTO users(id, username, messages) VALUES (%s, %s, %s)", (user_id, username, 0))
        db_connection.commit()

    update_messages_count(user_id)


@bot.message_handler(commands=["stats"])
def get_stats(message):
    db_object.execute("SELECT * FROM users ORDER BY messages DESC LIMIT 10")
    result = db_object.fetchall()
    bot.send_message(message.from_user.id, result)
    if not result:
        bot.reply_to(message, "No data...")
    else:
        reply_message = "- Top flooders:\n"
        for i, item in enumerate(result):
            reply_message += f"[{i + 1}] {item[1].strip()} ({item[0]}) : {item[2]} messages.\n"
        bot.reply_to(message, reply_message)
    update_messages_count(message.from_user.id)

@bot.message_handler(commands=["vsesmart"])
def get_vsesmart(message):
    bot.send_message(message.from_user.id, "Поехали, собираю VseSmart.....")
    try:
        parsing_vse_smart(bot, message)
    except Exception as ex:
        bot.send_message(message.from_user.id, ex)
    update_messages_count(message.from_user.id)

def search_count(category: str, message):
    st = f"https://mosprivoz.ru/catalog/{category}/"
    #bot.reply_to(message, st)
    try:
        db_object.execute(f"SELECT count(name) FROM public.mos_privoz_operational_metrics where links='{st}'")
    except Exception as ex:
        bot.send_message(message.from_user.id, ex)
    result = db_object.fetchall()
    if not result:
        bot.reply_to(message, "No data...")
    else:
        reply_message = f"- count: {result}"
        #for i, item in enumerate(result):
        #    reply_message += f"[{i + 1}] {item[1].strip()} ({item[0]}) : {item[2]} messages.\n"
        bot.reply_to(message, reply_message)
    update_messages_count(message.from_user.id)
def search_cat(message):
    try:
        db_object.execute(f"SELECT links, count(name) FROM public.mos_privoz_operational_metrics GROUP  BY links")
    except Exception as ex:
        bot.send_message(message.from_user.id, ex)
    result = db_object.fetchall()
    if not result:
        bot.reply_to(message, "No data...")
    else:
        reply_message = "- count: "
        for i, item in enumerate(result):
            reply_message += f"[{i + 1}] {item[0].strip()}  : {item[1]} skuss\n"
        bot.reply_to(message, reply_message)
    update_messages_count(message.from_user.id)
def search_poz(poz,message):
    try:
        db_object.execute(f"SELECT name, new_price, link FROM public.mos_privoz_operational_metrics where name like ('%{poz}%') Limit 10")
    except Exception as ex:
        bot.send_message(message.from_user.id, ex)
    result = db_object.fetchall()
    if not result:
        bot.reply_to(message, "No data...")
    else:
        reply_message = "- count: \n"
        for i, item in enumerate(result):
            reply_message += f"[{i + 1}] {item[0].strip()}  : {item[1]} : {item[2]}\n"
        bot.reply_to(message, reply_message)
    update_messages_count(message.from_user.id)


@bot.message_handler(func=lambda message: True, content_types=["text"])
def message_from_user(message):
    user_id = message.from_user.id
    update_messages_count(user_id)
    if message.text == "Привет":
        bot.send_message(message.from_user.id, f"Привет! {message.from_user.username}")
    elif message.text == "Запуск":
        bot.send_message(message.from_user.id, "Поехали.....")
        try:
            items = parsing_data()
        except Exception as ex:
            bot.send_message(message.from_user.id,ex)
        bot.send_message(message.from_user.id, f"Собрали {len(items)}")
        try:
            data_post_to_base(items)
        except Exception as ex:
            bot.send_message(message.from_user.id,ex)
        bot.send_message(message.from_user.id, f"Приехали.....{len(items)}")
    elif message.text[:5] == "Поиск":
        bot.send_message(message.from_user.id, "Считаю.....")
        bot.send_message(message.from_user.id, f"Ищу.....{message.text[6:]}")
        search_count(message.text[6:], message)
    elif message.text[:9] == "Категории":
        bot.send_message(message.from_user.id, "Считаю категории....")
        bot.send_message(message.from_user.id, f"Ищу.....{message.text[6:]}")
        search_cat(message)
    elif message.text[:7] == "Позиция":
        bot.send_message(message.from_user.id, "Считаю категории....")
        bot.send_message(message.from_user.id, f"Ищу.....{message.text[8:]}")
        search_poz(message.text[8:],message)
    #bot.send_message(message.from_user.id,message.text)


@server.route(f"/{BOT_TOKEN}", methods = ["POST"])
def redirect_message():
    json_string = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_string)
    bot.process_new_updates([update])
    return "!",200


if __name__ == "__main__":
    bot.remove_webhook()
    bot.set_webhook(url=APP_URL)
    server.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))