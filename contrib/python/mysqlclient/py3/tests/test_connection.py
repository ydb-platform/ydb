import pytest

from MySQLdb._exceptions import ProgrammingError

from configdb import connection_factory


def test_multi_statements_default_true():
    conn = connection_factory()
    cursor = conn.cursor()

    cursor.execute("select 17; select 2")
    rows = cursor.fetchall()
    assert rows == ((17,),)


def test_multi_statements_false():
    conn = connection_factory(multi_statements=False)
    cursor = conn.cursor()

    with pytest.raises(ProgrammingError):
        cursor.execute("select 17; select 2")

    cursor.execute("select 17")
    rows = cursor.fetchall()
    assert rows == ((17,),)
