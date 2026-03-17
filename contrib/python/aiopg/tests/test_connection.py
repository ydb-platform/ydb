import asyncio
import gc
import socket
import sys
import time
from unittest import mock

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pytest

import aiopg
from aiopg import DEFAULT_TIMEOUT, Connection, Cursor

PY_341 = sys.version_info >= (3, 4, 1)


@pytest.fixture
def connect(make_connection):
    async def go(**kwargs):
        return await make_connection(**kwargs)

    return go


async def test_connect(connect):
    conn = await connect()
    assert isinstance(conn, Connection)
    assert not conn._writing
    assert conn._conn is conn.raw
    assert not conn.echo


class TestMultipleHostsWithUnavailable:

    @pytest.fixture
    def pg_params(self, pg_params, pg_server, unused_port):
        pg_params = pg_params.copy()
        host = pg_params['host']
        port = pg_params['port']

        extra_host = "127.0.0.1"
        extra_port = unused_port()

        pg_params['host'] = '{extra_host},{host}'.format(
            extra_host=extra_host, host=host)
        pg_params['port'] = '{extra_port},{port}'.format(
            extra_port=extra_port, port=port)
        return pg_params

    async def test_connect(self, connect):
        # We should skip unavailable replica
        conn = await connect()
        assert isinstance(conn, Connection)
        assert not conn._writing
        assert conn._conn is conn.raw
        assert not conn.echo


class TestMultipleHostsWithStuckConnection:
    @pytest.yield_fixture
    def stuck_server_port(self, unused_port):
        # creates server which is not responding on SYN
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = unused_port()
        s.bind(('127.0.0.1', port))
        yield port
        s.close()

    @pytest.fixture
    def pg_params(self, pg_params, pg_server, stuck_server_port):
        pg_params = pg_params.copy()
        host = pg_params['host']
        port = pg_params['port']

        extra_host = "127.0.0.1"
        extra_port = stuck_server_port

        pg_params['host'] = '{extra_host},{host}'.format(
            extra_host=extra_host, host=host)
        pg_params['port'] = '{extra_port},{port}'.format(
            extra_port=extra_port, port=port)
        pg_params['connect_timeout'] = 1
        pg_params['timeout'] = 3

        return pg_params

    @pytest.mark.skipif(sys.platform != "linux",
                        reason='unstuck works only on linux')
    async def test_connect(self, connect):
        # We should skip unavailable replica
        conn = await connect()
        assert isinstance(conn, Connection)
        assert not conn._writing
        assert conn._conn is conn.raw
        assert not conn.echo


async def test_simple_select(connect):
    conn = await connect()
    cur = await conn.cursor()
    assert isinstance(cur, Cursor)
    await cur.execute("SELECT 1")
    ret = await cur.fetchone()
    assert (1,) == ret


async def test_simple_select_with_hstore(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute(
        """
        CREATE EXTENSION IF NOT EXISTS hstore;
        CREATE TABLE hfoo (id serial, hcol hstore);
        INSERT INTO hfoo (hcol) VALUES ('"col1"=>"456", "col2"=>"zzz"');
    """
    )

    # Reconnect because this is where the problem happens.
    cur.close()
    conn.close()
    conn = await connect(cursor_factory=psycopg2.extras.RealDictCursor)
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM hfoo;")
    ret = await cur.fetchone()
    await cur.execute("DROP TABLE hfoo;")
    assert {"hcol": {"col1": "456", "col2": "zzz"}, "id": 1} == ret


async def test_default_event_loop(connect, loop):
    asyncio.set_event_loop(loop)

    conn = await connect()
    cur = await conn.cursor()
    assert isinstance(cur, Cursor)
    await cur.execute("SELECT 1")
    ret = await cur.fetchone()
    assert (1,) == ret
    assert conn._loop is loop


async def test_close(connect):
    conn = await connect()
    await conn.close()
    assert conn.closed


async def test_close_twice(connect):
    conn = await connect()
    await conn.close()
    await conn.close()
    assert conn.closed


async def test_with_cursor_factory(connect):
    conn = await connect()
    cur = await conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    await cur.execute("SELECT 1 AS a")
    ret = await cur.fetchone()
    assert 1 == ret["a"]


async def test_closed(connect):
    conn = await connect()
    assert not conn.closed
    await conn.close()
    assert conn.closed


async def test_tpc(connect):
    conn = await connect()
    xid = await conn.xid(1, "a", "b")
    assert (1, "a", "b") == tuple(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.tpc_begin(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.tpc_prepare()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.tpc_commit(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.tpc_rollback(xid)

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.tpc_recover()


async def test_reset(connect):
    conn = await connect()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.reset()


async def test_lobject(connect):
    conn = await connect()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.lobject()


async def test_set_session(connect):
    conn = await connect()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.set_session()


async def test_dsn(connect, pg_params):
    conn = await connect()
    pg_params["password"] = "x" * len(pg_params["password"])
    assert "dbname" in conn.dsn
    assert "user" in conn.dsn
    assert "password" in conn.dsn
    assert "host" in conn.dsn
    assert "port" in conn.dsn


async def test_get_backend_pid(connect):
    conn = await connect()

    ret = await conn.get_backend_pid()
    assert 0 != ret


async def test_get_parameter_status(connect):
    conn = await connect()

    ret = await conn.get_parameter_status("integer_datetimes")
    assert "on" == ret


async def test_cursor_factory(connect):
    conn = await connect(cursor_factory=psycopg2.extras.DictCursor)

    assert psycopg2.extras.DictCursor is conn.cursor_factory


async def test_notices(connect):
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute("CREATE TABLE foo (id serial PRIMARY KEY);")

    if not conn.notices:
        raise pytest.skip("Notices are disabled")

    assert [
        "NOTICE:  CREATE TABLE will create implicit sequence "
        '"foo_id_seq" for serial column "foo.id"\n',
        "NOTICE:  CREATE TABLE / PRIMARY KEY will create "
        'implicit index "foo_pkey" for table "foo"\n',
    ] == conn.notices


async def test_autocommit(connect):
    conn = await connect()

    assert conn.autocommit

    with pytest.raises(psycopg2.ProgrammingError):
        conn.autocommit = False

    assert conn.autocommit


async def test_isolation_level(connect):
    conn = await connect()

    assert psycopg2.extensions.ISOLATION_LEVEL_DEFAULT == conn.isolation_level
    with pytest.raises(psycopg2.ProgrammingError):
        await conn.set_isolation_level(1)

    assert psycopg2.extensions.ISOLATION_LEVEL_DEFAULT == conn.isolation_level


async def test_encoding(connect):
    conn = await connect()

    assert "UTF8" == conn.encoding
    with pytest.raises(psycopg2.ProgrammingError):
        await conn.set_client_encoding("ascii")

    assert "UTF8" == conn.encoding


async def test_get_transaction_status(connect):
    conn = await connect()

    ret = await conn.get_transaction_status()
    assert 0 == ret


async def test_transaction(connect):
    conn = await connect()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.commit()

    with pytest.raises(psycopg2.ProgrammingError):
        await conn.rollback()


async def test_status(connect):
    conn = await connect()
    assert 1 == conn.status


async def test_protocol_version(connect):
    conn = await connect()
    assert 0 < conn.protocol_version


async def test_server_version(connect):
    conn = await connect()
    assert 0 < conn.server_version


async def test_cancel_not_supported(connect):
    conn = await connect()
    with pytest.raises(psycopg2.ProgrammingError):
        await conn.cancel()


async def test_close2(connect, loop):
    conn = await connect()
    conn._writing = True
    loop.add_writer(conn._fileno, conn._ready, conn._weakref)
    conn.close()
    assert not conn._writing
    assert conn.closed


async def test_psyco_exception(connect):
    conn = await connect()
    cur = await conn.cursor()
    with pytest.raises(psycopg2.ProgrammingError):
        await cur.execute("SELECT * FROM unknown_table")


def test_ready_set_exception(connect, loop):
    async def go():
        conn = await connect()
        impl = mock.Mock()
        impl.notifies = []
        exc = psycopg2.ProgrammingError("something bad")
        impl.poll.side_effect = exc
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter("test")

        conn._ready(conn._weakref)
        assert not conn._writing
        return waiter

    waiter = loop.run_until_complete(go())

    with pytest.raises(psycopg2.ProgrammingError):
        loop.run_until_complete(waiter)


def test_ready_OK_with_waiter(connect, loop):
    async def go():
        conn = await connect()
        impl = mock.Mock()
        impl.notifies = []
        impl.poll.return_value = psycopg2.extensions.POLL_OK

        # keep a reference to underlying psycopg connection, and the fd alive,
        # otherwise the event loop will fail under windows
        old_conn = conn._conn
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter("test")

        conn._ready(conn._weakref)
        assert not conn._writing
        assert not impl.close.called

        conn._conn = old_conn
        return waiter

    waiter = loop.run_until_complete(go())

    assert loop.run_until_complete(waiter) is None


def test_ready_POLL_ERROR(connect, loop):
    async def go():
        conn = await connect()
        impl = mock.Mock()
        impl.notifies = []
        impl.poll.return_value = psycopg2.extensions.POLL_ERROR
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter("test")
        handler = mock.Mock()
        loop.set_exception_handler(handler)

        conn._ready(conn._weakref)
        handler.assert_called_with(
            loop,
            {
                "connection": conn,
                "message": "Fatal error on aiopg connection: "
                "POLL_ERROR from underlying .poll() call",
            },
        )
        assert not conn._writing
        assert impl.close.called
        return waiter

    waiter = loop.run_until_complete(go())
    with pytest.raises(psycopg2.OperationalError):
        loop.run_until_complete(waiter)


def test_ready_unknown_answer(connect, loop):
    async def go():
        conn = await connect()
        impl = mock.Mock()
        impl.notifies = []
        impl.poll.return_value = 9999
        conn._conn = impl
        conn._writing = True
        waiter = conn._create_waiter("test")
        handler = mock.Mock()
        loop.set_exception_handler(handler)

        conn._ready(conn._weakref)
        handler.assert_called_with(
            loop,
            {
                "connection": conn,
                "message": "Fatal error on aiopg connection: "
                "unknown answer 9999 from underlying .poll() call",
            },
        )
        assert not conn._writing
        assert impl.close.called
        return waiter

    waiter = loop.run_until_complete(go())
    with pytest.raises(psycopg2.OperationalError):
        loop.run_until_complete(waiter)


async def test_execute_twice(connect):
    conn = await connect()
    cur1 = await conn.cursor()
    # cur2 = await conn.cursor()
    coro1 = cur1.execute("SELECT 1")
    fut1 = next(coro1.__await__())
    assert isinstance(fut1, asyncio.Future)
    coro2 = cur1.execute("SELECT 2")

    with pytest.raises(RuntimeError):
        next(coro2.__await__())


async def test_connect_to_unsupported_port(unused_port, loop, pg_params):
    port = unused_port()
    pg_params["port"] = port

    with pytest.raises(psycopg2.OperationalError):
        await aiopg.connect(**pg_params)


async def test_binary_protocol_error(connect):
    conn = await connect()
    s = socket.fromfd(conn._fileno, socket.AF_INET, socket.SOCK_STREAM)
    s.send(b"garbage")
    s.detach()
    cur = await conn.cursor()
    with pytest.raises(psycopg2.DatabaseError):
        await cur.execute("SELECT 1")


async def test_closing_in_separate_task(connect):
    closed_event = asyncio.Event()
    exec_created = asyncio.Event()

    async def waiter(conn):
        cur = await conn.cursor()
        fut = cur.execute("SELECT pg_sleep(1000)")
        exec_created.set()
        await closed_event.wait()
        with pytest.raises(psycopg2.InterfaceError):
            await fut

    async def closer(conn):
        await exec_created.wait()
        await conn.close()
        closed_event.set()

    conn = await connect()
    await asyncio.gather(waiter(conn), closer(conn))


async def test_connection_timeout(connect):
    timeout = 0.1
    conn = await connect(timeout=timeout)
    assert timeout == conn.timeout
    cur = await conn.cursor()
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_override_cursor_timeout(connect):
    timeout = 0.1
    conn = await connect()
    assert DEFAULT_TIMEOUT == conn.timeout
    cur = await conn.cursor(timeout=timeout)
    assert timeout == cur.timeout

    t1 = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(1)")
    t2 = time.time()
    dt = t2 - t1
    assert 0.08 <= dt <= 0.15, dt


async def test_echo(connect):
    conn = await connect(echo=True)
    assert conn.echo


async def test___del__(loop, pg_params, warning):
    exc_handler = mock.Mock()
    loop.set_exception_handler(exc_handler)
    conn = await aiopg.connect(**pg_params)
    with warning(ResourceWarning):
        del conn
        gc.collect()

    msg = {
        "connection": mock.ANY,  # conn was deleted
        "message": "Unclosed connection",
    }
    if loop.get_debug():
        msg["source_traceback"] = mock.ANY
        exc_handler.assert_called_with(loop, msg)


async def test_notifies(connect):
    conn1 = await connect()
    conn2 = await connect()

    async with await conn1.cursor() as cur1, await conn2.cursor() as cur2:
        await cur1.execute("LISTEN test")
        assert conn2.notifies.empty()
        await cur2.execute("NOTIFY test, 'hello'")
        val = await conn1.notifies.get()
        assert "test" == val.channel
        assert "hello" == val.payload

    await conn1.close()
    with pytest.raises(psycopg2.OperationalError):
        await conn1.notifies.get()


async def test_close_connection_on_timeout_error(connect):
    conn = await connect()
    cur = await conn.cursor(timeout=0.01)
    with pytest.raises(asyncio.TimeoutError):
        await cur.execute("SELECT pg_sleep(10)")

    assert conn.closed


async def test_issue_111_crash_on_connect_error():
    import aiopg.connection

    with pytest.raises(psycopg2.ProgrammingError):
        await aiopg.connection.connect("baddsn:1")


async def test_remove_reader_from_alive_fd(connect):
    conn = await connect()
    # keep a reference to underlying psycopg connection, and the fd alive
    _conn = conn._conn  # noqa
    fileno = conn._fileno

    impl = mock.Mock()
    exc = psycopg2.OperationalError("Test")
    impl.poll.side_effect = exc
    conn._conn = impl
    conn._fileno = fileno

    m_remove_reader = mock.Mock()
    conn._loop.remove_reader = m_remove_reader

    conn._ready(conn._weakref)
    assert not m_remove_reader.called

    conn.close()
    assert m_remove_reader.called_with(fileno)


async def test_remove_reader_from_dead_fd(connect):
    conn = await connect()
    fileno = conn._conn.fileno()
    _conn = conn._conn

    impl = mock.Mock()
    exc = psycopg2.OperationalError("Test")
    impl.poll.side_effect = exc
    conn._conn = impl
    conn._fileno = fileno

    _conn.close()

    m_remove_reader = mock.Mock()
    old_remove_reader = conn._loop.remove_reader
    conn._loop.remove_reader = m_remove_reader

    conn._ready(conn._weakref)
    assert m_remove_reader.called_with(fileno)

    m_remove_reader.reset_mock()
    conn.close()
    assert not m_remove_reader.called
    old_remove_reader(fileno)


async def test_connection_on_server_restart(connect, pg_server, docker):
    # Operation on closed connection should raise OperationalError
    conn = await connect()
    cur = await conn.cursor()
    await cur.execute("SELECT 1")
    ret = await cur.fetchone()
    assert (1,) == ret
    docker.restart(container=pg_server["Id"])

    with pytest.raises(psycopg2.OperationalError):
        await cur.execute("SELECT 1")
    conn.close()

    # Wait for postgres to be up and running again before moving on
    # so as the restart won't affect other tests
    delay = 0.001
    for i in range(100):
        try:
            conn = await connect()
            conn.close()
            break
        except psycopg2.Error:
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail("Cannot connect to the restarted server")


async def test_connection_notify_on_disconnect(
    connect, pg_params, tcp_proxy, unused_port, loop
):
    server_port = pg_params["port"]
    proxy_port = unused_port()
    tcp_proxy = await tcp_proxy(proxy_port, server_port)

    async with await connect(port=proxy_port) as connection:

        async def read_notifies(c):
            while True:
                await c.notifies.get()

        reader_task = loop.create_task(read_notifies(connection))
        await asyncio.sleep(0.1)

        await tcp_proxy.disconnect()
        try:
            with pytest.raises(psycopg2.OperationalError):
                await asyncio.wait_for(reader_task, 10)
        finally:
            reader_task.cancel()
