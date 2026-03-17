from __future__ import print_function, absolute_import

import pytest
import MySQLdb.cursors
from configdb import connection_factory


_conns = []
_tables = []

def connect(**kwargs):
    conn = connection_factory(**kwargs)
    _conns.append(conn)
    return conn


def teardown_function(function):
    if _tables:
        c = _conns[0]
        cur = c.cursor()
        for t in _tables:
            cur.execute("DROP TABLE %s" % (t,))
        cur.close()
        del _tables[:]

    for c in _conns:
        c.close()
    del _conns[:]


def test_executemany():
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("create table test (data varchar(10))")
    _tables.append("test")

    m = MySQLdb.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%s, %s)")
    assert m is not None, 'error parse %s'
    assert m.group(3) == '', 'group 3 not blank, bug in RE_INSERT_VALUES?'

    m = MySQLdb.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%(id)s, %(name)s)")
    assert m is not None, 'error parse %(name)s'
    assert m.group(3) == '', 'group 3 not blank, bug in RE_INSERT_VALUES?'

    m = MySQLdb.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%(id_name)s, %(name)s)")
    assert m is not None, 'error parse %(id_name)s'
    assert m.group(3) == '', 'group 3 not blank, bug in RE_INSERT_VALUES?'

    m = MySQLdb.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%(id_name)s, %(name)s) ON duplicate update")
    assert m is not None, 'error parse %(id_name)s'
    assert m.group(3) == ' ON duplicate update', 'group 3 not ON duplicate update, bug in RE_INSERT_VALUES?'

    # https://github.com/PyMySQL/mysqlclient-python/issues/178
    m = MySQLdb.cursors.RE_INSERT_VALUES.match("INSERT INTO bloup(foo, bar)VALUES(%s, %s)")
    assert m is not None

    # cursor._executed myst bee "insert into test (data) values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)"
    # list args
    data = range(10)
    cursor.executemany("insert into test (data) values (%s)", data)
    assert cursor._executed.endswith(b",(7),(8),(9)"), 'execute many with %s not in one query'

    # dict args
    data_dict = [{'data': i} for i in range(10)]
    cursor.executemany("insert into test (data) values (%(data)s)", data_dict)
    assert cursor._executed.endswith(b",(7),(8),(9)"), 'execute many with %(data)s not in one query'

    # %% in column set
    cursor.execute("""\
        CREATE TABLE percent_test (
            `A%` INTEGER,
            `B%` INTEGER)""")
    try:
        q = "INSERT INTO percent_test (`A%%`, `B%%`) VALUES (%s, %s)"
        assert MySQLdb.cursors.RE_INSERT_VALUES.match(q) is not None
        cursor.executemany(q, [(3, 4), (5, 6)])
        assert cursor._executed.endswith(b"(3, 4),(5, 6)"), "executemany with %% not in one query"
    finally:
        cursor.execute("DROP TABLE IF EXISTS percent_test")


def test_pyparam():
    conn = connect()
    cursor = conn.cursor()

    cursor.execute(u"SELECT %(a)s, %(b)s", {u'a': 1, u'b': 2})
    assert cursor._executed == b"SELECT 1, 2"
    cursor.execute(b"SELECT %(a)s, %(b)s", {b'a': 3, b'b': 4})
    assert cursor._executed == b"SELECT 3, 4"
