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