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

def get_user_messages(user_id, group_type):
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


def get_user_chats(user_id):
  try:
    with connect() as conn:
      with  conn.cursor() as cursor:
        cursor.execute("""
          SELECT c.id, c.aliase, c.name, CASE WHEN uc.user_id IS NULL THEN FALSE ELSE TRUE END
          FROM chat c
            LEFT JOIN user_chat uc ON uc.user_id = %s and uc.chat_id = c.id
          WHERE c.enabled""", (user_id,))
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

        cursor.execute("""
          UPDATE user SET sort = %s WHERE user_id = %s
        """, (data['user_id'], data['sort_option'],))

        conn.commit()				
        return True
  
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    return False
	
  finally:
    cursor.close()
    conn.close()