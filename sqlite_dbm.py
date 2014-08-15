'''
sqlite-dbm
==========
Dict-style DBM based on sqlite3.

    >>> import sqlite_dbm
    >>> db = sqlite_dbm.open(':memory:')
    >>> db['foo'] = ['bar', {'baz': 1}]
    >>> db['foo']
    ['bar', {'baz': 1}]
    >>> del db['foo']
    >>> len(db)
    0
    >>> db.close()

Fast insert:

    >>> db = sqlite_dbm.open(':memory:', auto_commit=False)
    >>> for i in range(1000):
    ...     db[str(i)] = 'foo'
    >>> db.commit()
    >>> len(db)
    1000
    >>> db.clear()
    >>> db.close()

sqlite_dbm.open options
-----------------------
- **filename** - first required argument
- **auto_commit=True** - auto commit after each db update
- **dumper='pickle'** - one of 'pickle', 'json' or 'marshal' or 'str'
- **compress_level=9** - if set it to 0, compression will be disabled
- **smart_compress=True** - compress only if compressed size less than raw
- **pickle_protocol=2** - see pickle docs
'''
from __future__ import print_function
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


__version__ = '3.2.0'

if sys.version_info < (3, ):
    pickle_loads = pickle.loads
    bytes = str
else:
    pickle_loads = lambda *a, **k: pickle.loads(*a, encoding='bytes', **k)
compress_prefix = 'z'.encode('utf-8')
uncompress_prefix = 'u'.encode('utf-8')


class SQLiteDBM(DictMixin):
    def __init__(self, filename, auto_commit=True, dumper='pickle',
                 compress_level=9, smart_compress=True, pickle_protocol=2):
        self.filename = filename
        self.auto_commit = auto_commit
        self.pickle_protocol = pickle_protocol
        self.compress_level = compress_level
        self.smart_compress = smart_compress
        assert dumper in ['pickle', 'json', 'marshal', 'str'], 'unknown dumper'
        if dumper == 'pickle':
            self.loads = pickle_loads
            self.dumps = self._pickle_dumps
        elif dumper == 'json':
            self.loads = lambda data: json.loads(data.decode('utf-8'))
            self.dumps = lambda data: json.dumps(data).encode('utf-8')
        elif dumper == 'marshal':
            self.loads = marshal.loads
            self.dumps = marshal.dumps
        else:
            self.loads = lambda data: data.decode('utf-8')
            self.dumps = lambda data: data.encode('utf-8')
        self.open()

    def _pickle_dumps(self, data):
        return pickle.dumps(data, protocol=self.pickle_protocol)

    def open(self):
        self.conn = sqlite3.connect(self.filename)
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
        fields = [
            'id     VARCHAR PRIMARY KEY',
            'value  BLOB',
        ]
        if self.compress_level and self.smart_compress:
            fields.append('compressed BOOLEAN')
        self.cur.execute('CREATE TABLE IF NOT EXISTS kv ( %s )' % (
            ',\n'.join(fields)))
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
        if self.compress_level and self.smart_compress:
            if data[2]:
                dump = zlib.decompress(data[1])
            else:
                dump = data[1]
        elif self.compress_level:
            dump = zlib.decompress(data[1])
        else:
            dump = data[1]
        return self.loads(bytes(dump))

    def __setitem__(self, key, value):
        dump = self.dumps(value)
        if self.compress_level:
            compressed_dump = zlib.compress(dump)
            if self.smart_compress:
                if len(compressed_dump) < len(dump):
                    value, compressed = compressed_dump, True
                else:
                    value, compressed = dump, False
                q = 'REPLACE INTO kv (id, value, compressed) VALUES (?, ?, ?)'
                self.cur.execute(q, (key, sqlite3.Binary(value), compressed))
            else:
                q = 'REPLACE INTO kv (id, value) VALUES (?, ?)'
                self.cur.execute(q, (key, sqlite3.Binary(compressed_dump)))
        else:
            q = 'REPLACE INTO kv (id, value) VALUES (?, ?)'
            self.cur.execute(q, (key, sqlite3.Binary(dump)))
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
    # doctest.testmod(verbose=True)
    # doctest.testmod()
    print(doctest.testmod(
        optionflags=doctest.REPORT_ONLY_FIRST_FAILURE
    ))
