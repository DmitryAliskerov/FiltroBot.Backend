from threading import Thread
from flask import Flask, render_template
from flask_cors import CORS

from requests import get_user_chats, get_user_sort

app = Flask('')
CORS(app)

@app.route('/')
def home():
  return render_template('index.html')

@app.route('/user_channels/<user_id>')
def user_channels(user_id):
  return {
    chats: get_user_chats(user_id),
    sortOption: get_user_sort(user_id)
  }

def run():
  app.run(host='0.0.0.0', port=80)

def run_flask():
  t = Thread(target=run, daemon = True)
  t.start()