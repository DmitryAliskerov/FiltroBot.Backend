import os
import psycopg2

from psycopg2.extras import execute_values

def connect():
  try:
    with psycopg2.connect(host=os.environ['DB_HOST'], dbname=os.environ['DB_NAME'], user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD']) as conn:
      return conn
  
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)

def get_user_chats(user_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT c.id, c.aliase, c.name, CASE WHEN uc.user_id IS NULL THEN FALSE ELSE TRUE END
          FROM chat c
            LEFT JOIN public.user_chat uc ON uc.user_id = %s and uc.chat_id = c.id
          WHERE c.enabled""", (user_id,))
        return cursor.fetchall()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def set_user_chats(username, data):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          INSERT INTO "user" (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (data['user_id'], username,))

        cursor.execute("""
          DELETE FROM user_chat WHERE user_id = %s AND chat_id=ANY(%s)
        """, (data['user_id'], data['chat_ids_to_delete'],))

        execute_values(cursor, """
          INSERT INTO user_chat (user_id, chat_id) VALUES %s ON CONFLICT DO NOTHING
        """, map(lambda x: (data['user_id'], x), data['chat_ids_to_insert']))

        conn.commit()				
        return True
  
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return False
	
  finally:
    cursor.close()
    conn.close()