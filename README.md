sqlite-dbm
==========
Dict-style DBM based on sqlite3.

    >>> import sqlite_dbm
    >>> db = sqlite_dbm.open('./test.sqlite')
    >>> db['foo'] = ['bar', 'baz', {'a': 1}]
    >>> db['foo']
    ['bar', 'baz', {'a': 1}]
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