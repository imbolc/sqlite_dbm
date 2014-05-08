'''
sqlite-dbm
==========
Dict-style DBM based on sqlite3.

    >>> import sqlite_dbm
    >>> db = sqlite_dbm.open('./test.sqlite')
    >>> db['foo'] = [1, 2, 3]
    >>> db['foo']
    [1, 2, 3]
    >>> del db['foo']
    >>> len(db)
    0
    >>> db.close()

Fast insert:

    >>> db = sqlite_dbm.open('./test.sqlite', auto_commit=False)
    >>> for i in range(1000):
    ...     db[str(i)] = 'foo'
    >>> db.commit()
    >>> len(db)
    1000
    >>> db.clear()
    >>> db.close()
'''
import sys
import zlib
import json
import pickle
import marshal
import sqlite3
try:
    from UserDict import DictMixin
except ImportError:
    from collections import MutableMapping as DictMixin


__version__ = '3.0.0'

if sys.version_info < (3, ):
    pickle_loads = pickle.loads
    bytes = str
else:
    pickle_loads = lambda *a, **k: pickle.loads(*a, encoding='bytes', **k)


class SQLiteDBM(DictMixin):
    def __init__(self, filename, auto_commit=True, dumper='json',
                 compress_level=0, pickle_protocol=2):
        self.filename = filename
        self.auto_commit = auto_commit
        self.pickle_protocol = pickle_protocol
        self.compress_level = compress_level
        assert dumper in ['pickle', 'json', 'marshal'], 'unknown dumper'
        if dumper == 'pickle':
            self.loads = pickle_loads
            self.dumps = self._pickle_dumps
        elif dumper == 'json':
            self.loads = lambda data: json.loads(data.decode('utf-8'))
            self.dumps = lambda data: json.dumps(data).encode('utf-8')
        else:
            self.loads = marshal.loads
            self.dumps = marshal.dumps
        self.open()

    def _pickle_dumps(self, data):
        return pickle.dumps(data, protocol=self.pickle_protocol)

    def open(self):
        self.conn = sqlite3.connect(self.filename)
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute(
            '''CREATE TABLE IF NOT EXISTS kv (
                id     VARCHAR PRIMARY KEY,
                value  VARCHAR
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
        data = data[1]
        data = bytes(data)
        if self.compress_level:
            data = zlib.decompress(data)
        return self.loads(data)

    def __setitem__(self, key, value):
        value = self.dumps(value)
        if self.compress_level:
            value = zlib.compress(value)
        value = sqlite3.Binary(value)
        self.cur.execute(
            'REPLACE INTO kv (id, value) VALUES (?, ?)', (
                key, value))
        if self.auto_commit:
            self.conn.commit()

    def __delitem__(self, key):
        self.cur.execute('DELETE FROM kv WHERE id=?', (key, ))
        if self.auto_commit:
            self.conn.commit()

    def keys(self):
        return list(self)

    def __len__(self):
        self.cur.execute('SELECT count(*) FROM kv')
        return self.cur.fetchone()[0]

    def __iter__(self):
        self.cur.execute('SELECT id FROM kv')
        return (r[0] for r in self.cur.fetchall())


def open(*args, **kwargs):
    return SQLiteDBM(*args, **kwargs)


if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)
