import os
import json 
import time
import requests
import asyncio

from telethon import TelegramClient, events
from telethon.sync import TelegramClient
from telethon import functions, types
from telethon import Button
from telethon.tl.custom import Button
from flask_server import run_flask
from sender import run_sender
from threading import Thread

bot = TelegramClient('bot', api_id=os.environ['API_ID'], api_hash=os.environ['API_HASH']).start(bot_token=os.environ['BOT_TOKEN'])
base_url = "https://dmitryaliskerov.github.io/FiltroBot.UI"

@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
	sender = await event.get_sender()
	user_id = sender.id

	button_url = f"{base_url}?user_id={user_id}"
	if requests.has_user_at_least_one_chat(user_id):
		await bot.send_message(event.chat_id, f'Приветствую Вас, {sender.username}. Мы подготовили для Вас сообщения по выбранным каналам.', buttons=[types.KeyboardButtonSimpleWebView('Управлять каналами', button_url)])
	else:
		requests.set_user(sender.id, sender.username)
		await bot.send_message(event.chat_id, f'Приветствую Вас, {sender.username}. У Вас пока не выбран ни один канал.', buttons=[types.KeyboardButtonSimpleWebView('Выбрать каналы', button_url)])
    
	raise events.StopPropagation

@bot.on(events.CallbackQuery) # filter
async def handler(callback):

	data = json.loads(callback.data.decode("utf-8"))

	messages = requests.get_chat_messages(data[0])

	for message in messages:
		await bot.send_message(data[1], f"<b>{message[0]}</b>      {message[2]}\n\n{message[1]}", parse_mode='html')	

	raise events.StopPropagation

@bot.on(events.Raw)
async def handler(update):
	print(update)

	if not hasattr(update, 'message') or type(update.message).__name__ != "MessageService":
		return

	data = json.loads(update.message.action.data)
	user_id = update.message.peer_id.user_id
	entity = await bot.get_entity(user_id)

	if str(entity.id) != data['user_id']:
		await bot.send_message(entity=entity, message="Что-то пошло не так. Идентификатор пользователя не совпадает.")
		return

	if requests.set_user_settings(data):
		button_url = f"{base_url}?user_id={user_id}"
		if requests.has_user_at_least_one_chat(user_id):
			await bot.send_message(entity=entity, message="Каналы успешно сохранены. Через некоторое время будут сформированы данные.", buttons=[types.KeyboardButtonSimpleWebView('Управлять каналами', button_url)])
		else:
			await bot.send_message(entity=entity, message="Список каналов пуст.", buttons=[types.KeyboardButtonSimpleWebView('Выбрать каналы', button_url)])
	else:
		await bot.send_message(entity=entity, message="Ошибка сохранения данных.")

	raise events.StopPropagation

run_flask()
run_sender(bot, 300)

def main():
	bot.run_until_disconnected()

if __name__ == '__main__':
	main()