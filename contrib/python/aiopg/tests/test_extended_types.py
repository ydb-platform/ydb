import uuid

from psycopg2.extras import Json


async def test_uuid(make_connection):
    conn = await make_connection()
    _id = uuid.uuid1()
    cur = await conn.cursor()
    try:
        await cur.execute("DROP TABLE IF EXISTS tbl")
        await cur.execute("""CREATE TABLE tbl (id UUID)""")
        await cur.execute("INSERT INTO tbl (id) VALUES (%s)", [_id])
        await cur.execute("SELECT * FROM tbl")
        item = await cur.fetchone()
        assert (_id,) == item
    finally:
        cur.close()


async def test_json(make_connection):
    conn = await make_connection()
    data = {"a": 1, "b": "str"}
    cur = await conn.cursor()
    try:
        await cur.execute("DROP TABLE IF EXISTS tbl")
        await cur.execute(
            """CREATE TABLE tbl (
                              id SERIAL,
                              val JSON)"""
        )
        await cur.execute("INSERT INTO tbl (val) VALUES (%s)", [Json(data)])
        await cur.execute("SELECT * FROM tbl")
        item = await cur.fetchone()
        assert (1, {"b": "str", "a": 1}) == item
    finally:
        cur.close()
