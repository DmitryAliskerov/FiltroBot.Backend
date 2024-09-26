import time
import requests
import operator
import itertools
import asyncio

from threading import Thread

def get_max_timestamp_by_chat_id(messages):
    list = []
    it = itertools.groupby(messages, operator.itemgetter(4))
    for key, subiter in it:
       list.append((key, max(item[5] for item in subiter)))
    return list

async def run_sender(bot, interval):
	while True:

		print("Run sender")

		start_time = time.time()
        
		users = requests.get_users()

		for user in users:
			entity = await bot.get_entity(user[0])
			if entity.status != None:
				messages = requests.get_user_messages(user[0], user[1])
				for message in messages:
					try:
						await bot.send_message(user[0], f"<b>{message[0]}</b>\n{message[1]}\n\n{message[2]}", parse_mode='html')
					except Exception as e:
						break

				data = get_max_timestamp_by_chat_id(messages)
				requests.set_user_chat_messages(user[0], data)

		elapsed = time.time() - start_time

		print("Sender sleeping...")

		if (elapsed < interval): await asyncio.sleep(interval - elapsed)