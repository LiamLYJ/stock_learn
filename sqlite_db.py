import json
import sqlite3

class data_base:
    def __init__(self, file_name):
        self.conn = sqlite3.connect(file_name)
        self.cur = self.conn.cursor()
        self.cur.executescript('''
        DROP TABLE IF EXISTS Name;
        DROP TABLE IF EXISTS Date;
        DROP TABLE IF EXISTS Data;

        CREATE TABLE Name (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            name TEXT UNIQUE
        );

        CREATE TABLE Date (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            date TEXT UNIQUE
        );

        CREATE TABLE Data(
            name_id INTEGER,
            date_id INTEGER,
            open_price real,
            max_price real,
            min_price real,
            close_price real,
            price_range real,
            total_volume real,
            total_money real
        )
        ''')

    def commit(self):
        self.conn.commit()

    def add_key(self, key):
        self.cur.execute('''INSERT OR IGNORE INTO Name (name)
        VALUES ( ? )''', ( key, ) )

    # data is a dict
    def add_data(self, data, key):
        self.cur.execute('''INSERT OR IGNORE INTO Date (date)
        VALUES ( ? )''', ( data['cur_timer'], ) )

        self.cur.execute('SELECT id FROM Name WHERE name = ? ', (key, ))
        name_id = self.cur.fetchone()[0]
        self.cur.execute('SELECT id FROM Date WHERE date = ? ', (data['cur_timer'], ))
        date_id = self.cur.fetchone()[0]

        self.cur.execute('''INSERT INTO Data
                (
                    name_id ,
                    date_id ,
                    open_price ,
                    max_price ,
                    min_price ,
                    close_price ,
                    price_range ,
                    total_volume ,
                    total_money
                ) VALUES (?,?,?,?,?,?,?,?,?)''',
                (
                    name_id, date_id,
                    data["cur_open_price"],
                    data["cur_max_price"],
                    data["cur_min_price"],
                    data["cur_close_price"],
                    data["cur_price_range"],
                    data["cur_total_volume"],
                    data["cur_total_money"]
                )
                )
