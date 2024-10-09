import os
import psycopg2

from psycopg2.extras import execute_values

def connect():
  try:
    with psycopg2.connect(host=os.environ['DB_HOST'], dbname=os.environ['DB_NAME'], user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD']) as conn:
      return conn
  
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)


def has_user_at_least_one_chat(user_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT chat_id
          FROM user_chat
          WHERE user_id = %s LIMIT 1""", (user_id,))
        return cursor.fetchone() is not None

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def get_user_chat_stats(user_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT uc.chat_id, c.name, count(m.id)
          FROM user_chat uc
	  	LEFT JOIN chat c ON c.id = uc.chat_id
		LEFT JOIN "message" m ON m.chat_id = uc.chat_id
          WHERE user_id = %s
	  GROUP BY uc.chat_id, c.name""", (user_id,))
        return cursor.fetchall()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def get_chat_messages(chat_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT t.name, "link", m.timestamp
          FROM message m
            LEFT JOIN message_tag mt ON mt.id = m.id AND mt.chat_id = m.chat_id
            LEFT JOIN tag t ON t.id = mt.tag_id
          WHERE m.chat_id = %s
          ORDER BY m.timestamp""", (chat_id,))
        return cursor.fetchall()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def get_user_messages(user_id, sort_option):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        order = "ORDER BY "
        if sort_option == 0:
          order += "m.timestamp"
        elif sort_option == 1:
          order += "m.chat_id, m.timestamp"
        elif sort_option == 2:
          order += "m.chat_id, t.name, m.timestamp"
        elif sort_option == 3:
          order += "t.name, m.timestamp"
        elif sort_option == 4:
          order += "t.name, m.chat_id, m.timestamp"

        cursor.execute("""
          SELECT COALESCE(t.name, c.theme), m.timestamp - INTERVAL '1 minute' * u.tz_offset, m.link, m.id, m.chat_id, m.timestamp
          FROM "user" u
            INNER JOIN user_chat uc ON uc.user_id = u.id
            LEFT JOIN chat c ON c.id = uc.chat_id
            LEFT JOIN message m ON m.chat_id = uc.chat_id
            LEFT JOIN message_tag mt ON mt.id = m.id AND mt.chat_id = m.chat_id
            LEFT JOIN tag t ON t.id = mt.tag_id
          WHERE u.id = %s AND m.timestamp > COALESCE(uc.timestamp, now() at time zone 'utc' - INTERVAL '1 hour')""" + "\n" + order, (user_id,))
        return cursor.fetchall()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def get_users():
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("SELECT id, sort FROM public.\"user\"")
        return cursor.fetchall()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def get_user_chats(user_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT c.id, c.theme, c.name, CASE WHEN uc.user_id IS NULL THEN FALSE ELSE TRUE END, c.description
          FROM chat c
            LEFT JOIN user_chat uc ON uc.user_id = %s and uc.chat_id = c.id
          WHERE c.enabled
          ORDER BY c.name""", (user_id,))
        return cursor.fetchall()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()

def get_user_sort(user_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT sort
          FROM "user"
          WHERE id = %s""", (user_id,))
        return cursor.fetchone()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return -1
  
  finally:
    cursor.close()
    conn.close()
               
def set_user(user_id, user_name):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          INSERT INTO "user" (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (user_id, user_name,))

        conn.commit()				
        return True
  
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return False
	
  finally:
    cursor.close()
    conn.close()

def set_user_chat_messages(user_id, data):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        execute_values(cursor, """
          UPDATE user_chat SET timestamp = data.timestamp
          FROM (VALUES %s) AS data (user_id, chat_id, timestamp)
          WHERE user_chat.user_id = data.user_id AND user_chat.chat_id = data.chat_id
        """, map(lambda x: (user_id, x[0], x[1]), data))

        conn.commit()				
        return True
  
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return False
	
  finally:
    cursor.close()
    conn.close()


def set_user_settings(data):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          DELETE FROM user_chat WHERE user_id = %s AND chat_id=ANY(%s)
        """, (data['user_id'], data['chat_ids_to_delete'],))

        execute_values(cursor, """
          INSERT INTO user_chat (user_id, chat_id) VALUES %s ON CONFLICT DO NOTHING
        """, map(lambda x: (data['user_id'], x), data['chat_ids_to_insert']))

        cursor.execute("""
          UPDATE "user" SET sort = %s, tz_offset = %s WHERE id = %s
        """, (data['sort_option'], data['tz_offset'], data['user_id'],))

        conn.commit()				
        return True
  
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return False
	
  finally:
    cursor.close()
    conn.close()

