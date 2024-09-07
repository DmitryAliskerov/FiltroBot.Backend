import os
import json 

from telethon import TelegramClient, events
from telethon.sync import TelegramClient
from telethon import functions, types
from telethon.tl.custom import Button
from flask_server import run_flask
from requests import set_user_chats

bot = TelegramClient('bot', api_id=os.environ['API_ID'], api_hash=os.environ['API_HASH']).start(bot_token=os.environ['BOT_TOKEN'])

@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
	sender = await event.get_sender()
	await bot.send_message(event.chat_id, f'Приветствую Вас, {sender.username}', buttons=[types.KeyboardButtonSimpleWebView('Test', f"https://dmitryaliskerov.github.io/FiltroBot.UI?user_id={sender.id}")])
    
	raise events.StopPropagation

@bot.on(events.Raw)
async def handler(update):
	print("MessageService")

	if type(update.message).__name__ != "MessageService":
		return

	print("MessageService Proccess")

	data = json.loads(update.message.action.data)
	user_id = update.message.peer_id.user_id
	entity = await bot.get_entity(user_id)

	if str(entity.id) != data['user_id']:
		await bot.send_message(entity=entity, message="Что-то пошло не так. Идентификатор пользователя не совпадает.")
		return

	if set_user_chats(entity.username, data):
		await bot.send_message(entity=entity, message="Данные сохранены")
	else:
		await bot.send_message(entity=entity, message="Ошибка сохранения данных")

	raise events.StopPropagation

run_flask()

def main():
	bot.run_until_disconnected()

if __name__ == '__main__':
	main()