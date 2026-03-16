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
            cur.execute(f"DROP TABLE {t}")
        cur.close()
        del _tables[:]

    for c in _conns:
        c.close()
    del _conns[:]


def test_null():
    """Inserting NULL into non NULLABLE column"""
    # https://github.com/PyMySQL/mysqlclient/issues/535
    table_name = "test_null"
    conn = connect()
    cursor = conn.cursor()

    cursor.execute(f"create table {table_name} (c1 int primary key)")
    _tables.append(table_name)

    with pytest.raises(MySQLdb.IntegrityError):
        cursor.execute(f"insert into {table_name} values (null)")


def test_duplicated_pk():
    """Inserting row with duplicated PK"""
    # https://github.com/PyMySQL/mysqlclient/issues/535
    table_name = "test_duplicated_pk"
    conn = connect()
    cursor = conn.cursor()

    cursor.execute(f"create table {table_name} (c1 int primary key)")
    _tables.append(table_name)

    cursor.execute(f"insert into {table_name} values (1)")
    with pytest.raises(MySQLdb.IntegrityError):
        cursor.execute(f"insert into {table_name} values (1)")
