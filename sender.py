import time
import requests
import operator
import itertools

from threading import Thread

def get_max_message_id_by_chat_id(messages):
    it = itertools.groupby(messages, operator.itemgetter(4))
    for key, subiter in it:
       yield key, max(item[3] for item in subiter) 

def run(bot, interval):
	while True:
		start_time = time.time()

		elapsed = time.time() - start_time

		users = requests.get_users()

		for user in users:
			entity = bot.get_entity(user[0])
			if entity.status != None:
				messages = requests.get_user_messages(user[0], user[1])
				for message in messages:
					try:
						bot.send_message(user[0], f"<b>{message[0]}</b>      {message[2]}\n\n{message[1]}", parse_mode='html')
					except Exception as e:
						break

				ids = get_max_message_id_by_chat_id(messages)
				requests.set_user_chat_messages(user[0], list(ids))

		if (elapsed < interval): time.sleep(interval - elapsed)


def run_sender(bot, interval):
	with bot:
		bot.loop.run_until_complete(run(bot, interval))


