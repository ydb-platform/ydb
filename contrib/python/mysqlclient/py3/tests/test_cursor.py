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


def test_executemany():
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("create table test (data varchar(10))")
    _tables.append("test")

    m = MySQLdb.cursors.RE_INSERT_VALUES.match(
        "INSERT INTO TEST (ID, NAME) VALUES (%s, %s)"
    )
    assert m is not None, "error parse %s"
    assert m.group(3) == "", "group 3 not blank, bug in RE_INSERT_VALUES?"

    m = MySQLdb.cursors.RE_INSERT_VALUES.match(
        "INSERT INTO TEST (ID, NAME) VALUES (%(id)s, %(name)s)"
    )
    assert m is not None, "error parse %(name)s"
    assert m.group(3) == "", "group 3 not blank, bug in RE_INSERT_VALUES?"

    m = MySQLdb.cursors.RE_INSERT_VALUES.match(
        "INSERT INTO TEST (ID, NAME) VALUES (%(id_name)s, %(name)s)"
    )
    assert m is not None, "error parse %(id_name)s"
    assert m.group(3) == "", "group 3 not blank, bug in RE_INSERT_VALUES?"

    m = MySQLdb.cursors.RE_INSERT_VALUES.match(
        "INSERT INTO TEST (ID, NAME) VALUES (%(id_name)s, %(name)s) ON duplicate update"
    )
    assert m is not None, "error parse %(id_name)s"
    assert (
        m.group(3) == " ON duplicate update"
    ), "group 3 not ON duplicate update, bug in RE_INSERT_VALUES?"

    # https://github.com/PyMySQL/mysqlclient-python/issues/178
    m = MySQLdb.cursors.RE_INSERT_VALUES.match(
        "INSERT INTO bloup(foo, bar)VALUES(%s, %s)"
    )
    assert m is not None

    # cursor._executed myst bee
    # """
    # insert into test (data)
    # values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
    # """
    # list args
    data = [(i,) for i in range(10)]
    cursor.executemany("insert into test (data) values (%s)", data)
    assert cursor._executed.endswith(
        b",(7),(8),(9)"
    ), "execute many with %s not in one query"

    # dict args
    data_dict = [{"data": i} for i in range(10)]
    cursor.executemany("insert into test (data) values (%(data)s)", data_dict)
    assert cursor._executed.endswith(
        b",(7),(8),(9)"
    ), "execute many with %(data)s not in one query"

    # %% in column set
    cursor.execute(
        """\
        CREATE TABLE percent_test (
            `A%` INTEGER,
            `B%` INTEGER)"""
    )
    try:
        q = "INSERT INTO percent_test (`A%%`, `B%%`) VALUES (%s, %s)"
        assert MySQLdb.cursors.RE_INSERT_VALUES.match(q) is not None
        cursor.executemany(q, [(3, 4), (5, 6)])
        assert cursor._executed.endswith(
            b"(3, 4),(5, 6)"
        ), "executemany with %% not in one query"
    finally:
        cursor.execute("DROP TABLE IF EXISTS percent_test")


def test_pyparam():
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("SELECT %(a)s, %(b)s", {"a": 1, "b": 2})
    assert cursor._executed == b"SELECT 1, 2"
    cursor.execute(b"SELECT %(a)s, %(b)s", {b"a": 3, b"b": 4})
    assert cursor._executed == b"SELECT 3, 4"


def test_dictcursor():
    conn = connect()
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute("CREATE TABLE t1 (a int, b int, c int)")
    _tables.append("t1")
    cursor.execute("INSERT INTO t1 (a,b,c) VALUES (1,1,47), (2,2,47)")

    cursor.execute("CREATE TABLE t2 (b int, c int)")
    _tables.append("t2")
    cursor.execute("INSERT INTO t2 (b,c) VALUES (1,1), (2,2)")

    cursor.execute("SELECT * FROM t1 JOIN t2 ON t1.b=t2.b")
    rows = cursor.fetchall()

    assert len(rows) == 2
    assert rows[0] == {"a": 1, "b": 1, "c": 47, "t2.b": 1, "t2.c": 1}
    assert rows[1] == {"a": 2, "b": 2, "c": 47, "t2.b": 2, "t2.c": 2}

    names1 = sorted(rows[0])
    names2 = sorted(rows[1])
    for a, b in zip(names1, names2):
        assert a is b

    # Old fetchtype
    cursor._fetch_type = 2
    cursor.execute("SELECT * FROM t1 JOIN t2 ON t1.b=t2.b")
    rows = cursor.fetchall()

    assert len(rows) == 2
    assert rows[0] == {"t1.a": 1, "t1.b": 1, "t1.c": 47, "t2.b": 1, "t2.c": 1}
    assert rows[1] == {"t1.a": 2, "t1.b": 2, "t1.c": 47, "t2.b": 2, "t2.c": 2}

    names1 = sorted(rows[0])
    names2 = sorted(rows[1])
    for a, b in zip(names1, names2):
        assert a is b


def test_mogrify_without_args():
    conn = connect()
    cursor = conn.cursor()

    query = "SELECT VERSION()"
    mogrified_query = cursor.mogrify(query)
    cursor.execute(query)

    assert mogrified_query == query
    assert mogrified_query == cursor._executed.decode()


def test_mogrify_with_tuple_args():
    conn = connect()
    cursor = conn.cursor()

    query_with_args = "SELECT %s, %s", (1, 2)
    mogrified_query = cursor.mogrify(*query_with_args)
    cursor.execute(*query_with_args)

    assert mogrified_query == "SELECT 1, 2"
    assert mogrified_query == cursor._executed.decode()


def test_mogrify_with_dict_args():
    conn = connect()
    cursor = conn.cursor()

    query_with_args = "SELECT %(a)s, %(b)s", {"a": 1, "b": 2}
    mogrified_query = cursor.mogrify(*query_with_args)
    cursor.execute(*query_with_args)

    assert mogrified_query == "SELECT 1, 2"
    assert mogrified_query == cursor._executed.decode()


# Test that cursor can be used without reading whole resultset.
@pytest.mark.parametrize("Cursor", [MySQLdb.cursors.Cursor, MySQLdb.cursors.SSCursor])
def test_cursor_discard_result(Cursor):
    conn = connect()
    cursor = conn.cursor(Cursor)

    cursor.execute(
        """\
CREATE TABLE test_cursor_discard_result (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    data VARCHAR(100)
)"""
    )
    _tables.append("test_cursor_discard_result")

    cursor.executemany(
        "INSERT INTO test_cursor_discard_result (id, data) VALUES (%s, %s)",
        [(i, f"row {i}") for i in range(1, 101)],
    )

    cursor.execute(
        """\
SELECT * FROM test_cursor_discard_result WHERE id <= 10;
SELECT * FROM test_cursor_discard_result WHERE id BETWEEN 11 AND 20;
SELECT * FROM test_cursor_discard_result WHERE id BETWEEN 21 AND 30;
"""
    )
    cursor.nextset()
    assert cursor.fetchone() == (11, "row 11")

    cursor.execute(
        "SELECT * FROM test_cursor_discard_result WHERE id BETWEEN 31 AND 40"
    )
    assert cursor.fetchone() == (31, "row 31")


def test_binary_prefix():
    # https://github.com/PyMySQL/mysqlclient/issues/494
    conn = connect(binary_prefix=True)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS test_binary_prefix")
    cursor.execute(
        """\
CREATE TABLE test_binary_prefix (
	id INTEGER NOT NULL AUTO_INCREMENT,
	json JSON NOT NULL,
	PRIMARY KEY (id)
) CHARSET=utf8mb4"""
    )

    cursor.executemany(
        "INSERT INTO test_binary_prefix (id, json) VALUES (%(id)s, %(json)s)",
        ({"id": 1, "json": "{}"}, {"id": 2, "json": "{}"}),
    )
