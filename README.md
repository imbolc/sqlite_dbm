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
- **dumper='pickle'** - one of 'pickle', 'json' or 'marshal'
- **compress_level=9** - if set it to 0, compression will be disabled
- **smart_compress=True** - compress only if compressed size less than raw
- **pickle_protocol=2** - see pickle docs