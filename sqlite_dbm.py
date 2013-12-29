'''
sqlite-dbm
==========
Dict-style DBM based on sqlite3.

    >>> import sqlite_dbm
    >>> db = sqlite_dbm.open('./test.sqlite')
    >>> db['foo'] = 'bar'
    >>> db['foo']
    'bar'
    >>> del db['foo']
    >>> len(db)
    0
    >>> db.close()

Fast insert:

    >>> db = sqlite_dbm.open('./test.sqlite', auto_commit=False)
    >>> for i in xrange(1000):
    ...     db[str(i)] = 'foo'
    >>> db.commit()
    >>> len(db)
    1000
    >>> db.clear()
    >>> db.close()
'''
import sqlite3
from UserDict import DictMixin


__version__ = '1.1.0'


class SQLiteDBM(DictMixin):
    def __init__(self, filename, auto_commit=True):
        self.filename = filename
        self.auto_commit = auto_commit
        self.open()

    def open(self):
        self.conn = sqlite3.connect(self.filename)
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute(
            '''CREATE TABLE IF NOT EXISTS kv (
                id    VARCHAR PRIMARY KEY,
                value VARCHAR
            )''')
        self.conn.commit()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.execute("VACUUM")
        self.conn.close()

    def __getitem__(self, key):
        self.cur.execute('SELECT * FROM kv WHERE id=?', (key, ))
        data = self.cur.fetchone()
        if not data:
            raise KeyError(key)
        return str(data[1]).decode('zlib')

    def __setitem__(self, key, value):
        if not isinstance(value, str):
            raise ValueError('value must be a string')
        value = sqlite3.Binary(value.encode('zlib'))
        self.cur.execute(
            'REPLACE INTO kv (id, value) VALUES (?, ?)', (key, value))
        if self.auto_commit:
            self.conn.commit()

    def __delitem__(self, key):
        self.cur.execute('DELETE FROM kv WHERE id=?', (key, ))
        if self.auto_commit:
            self.conn.commit()

    def keys(self):
        self.cur.execute('SELECT id FROM kv')
        return [r[0] for r in self.cur.fetchall()]


def open(*args, **kwargs):
    return SQLiteDBM(*args, **kwargs)


if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
