import sqlite3
import time

def create_db(db_name='prices.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS pricesTS (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        set_name TEXT,
        pack_sold TEXT,
        current_price TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()


def ingestData(data, db):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    ts = int(time.time())

    query = """
    INSERT INTO cards (timestamp, set_name, pack_sold, current_price)
    VALUES (?, ?, ?, ?);
    """

    for key in data:
        cursor.execute(query, (ts, key, data[key]['sold'], data[key]['current']))
    conn.commit()
    conn.close()

