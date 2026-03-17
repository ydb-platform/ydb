import asyncio
import ipaddress

import pytest

import aioftp


@pytest.mark.asyncio
async def test_client_without_server(pair_factory, unused_tcp_port_factory):
    f = pair_factory(connected=False, logged=False, do_quit=False)
    async with f as pair:
        pass
    with pytest.raises(OSError):
        await pair.client.connect("127.0.0.1", unused_tcp_port_factory())


@pytest.mark.asyncio
async def test_connection(pair_factory):
    async with pair_factory(connected=True, logged=False, do_quit=False):
        pass


@pytest.mark.asyncio
async def test_quit(pair_factory):
    async with pair_factory(connected=True, logged=False, do_quit=True):
        pass


@pytest.mark.asyncio
async def test_not_implemented(pair_factory, expect_codes_in_exception):
    async with pair_factory() as pair:
        with expect_codes_in_exception("502"):
            await pair.client.command("FOOBAR", "2xx", "1xx")


@pytest.mark.asyncio
async def test_type_success(pair_factory, expect_codes_in_exception):
    async with pair_factory() as pair:
        await pair.client.get_passive_connection("A")


@pytest.mark.asyncio
async def test_custom_passive_commands(pair_factory):
    async with pair_factory(host="127.0.0.1") as pair:
        pair.client._passive_commands = None
        await pair.client.get_passive_connection(
            "A",
            commands=["pasv", "epsv"],
        )


@pytest.mark.asyncio
async def test_extra_pasv_connection(pair_factory):
    async with pair_factory() as pair:
        r, w = await pair.client.get_passive_connection()
        er, ew = await pair.client.get_passive_connection()
        with pytest.raises((ConnectionResetError, BrokenPipeError)):
            while True:
                w.write(b"-" * aioftp.DEFAULT_BLOCK_SIZE)
                await w.drain()


@pytest.mark.parametrize("method", ["epsv", "pasv"])
@pytest.mark.asyncio
async def test_closing_passive_connection(pair_factory, method):
    async with pair_factory(host="127.0.0.1") as pair:
        r, w = await pair.client.get_passive_connection(commands=[method])
        host, port, *_ = w.transport.get_extra_info("peername")
        nr, nw = await asyncio.open_connection(host, port)
        with pytest.raises((ConnectionResetError, BrokenPipeError)):
            while True:
                nw.write(b"-" * aioftp.DEFAULT_BLOCK_SIZE)
                await nw.drain()


@pytest.mark.asyncio
async def test_pasv_connection_ports_not_added(pair_factory):
    async with pair_factory() as pair:
        r, w = await pair.client.get_passive_connection()
        assert pair.server.available_data_ports is None


@pytest.mark.asyncio
async def test_pasv_connection_ports(
    pair_factory,
    Server,
    unused_tcp_port_factory,
):
    ports = [unused_tcp_port_factory(), unused_tcp_port_factory()]
    async with pair_factory(None, Server(data_ports=ports)) as pair:
        r, w = await pair.client.get_passive_connection()
        host, port, *_ = w.transport.get_extra_info("peername")
        assert port in ports
        assert pair.server.available_data_ports.qsize() == 1


@pytest.mark.asyncio
async def test_data_ports_remains_empty(pair_factory, Server):
    async with pair_factory(None, Server(data_ports=[])) as pair:
        assert pair.server.available_data_ports.qsize() == 0


@pytest.mark.asyncio
async def test_pasv_connection_port_reused(
    pair_factory,
    Server,
    unused_tcp_port,
):
    s = Server(data_ports=[unused_tcp_port])
    async with pair_factory(None, s) as pair:
        r, w = await pair.client.get_passive_connection()
        host, port, *_ = w.transport.get_extra_info("peername")
        assert port == unused_tcp_port
        assert pair.server.available_data_ports.qsize() == 0
        w.close()
        await pair.client.quit()
        pair.client.close()
        assert pair.server.available_data_ports.qsize() == 1
        await pair.client.connect(
            pair.server.server_host,
            pair.server.server_port,
        )
        await pair.client.login()
        r, w = await pair.client.get_passive_connection()
        host, port, *_ = w.transport.get_extra_info("peername")
        assert port == unused_tcp_port
        assert pair.server.available_data_ports.qsize() == 0


@pytest.mark.asyncio
async def test_pasv_connection_pasv_forced_response_address(pair_factory, Server):
    def ipv4_used():
        try:
            ipaddress.IPv4Address(pair.host)
            return True
        except ValueError:
            return False

    # using TEST-NET-1 address
    ipv4_address = "192.0.2.1"
    async with pair_factory(
        server=Server(ipv4_pasv_forced_response_address=ipv4_address),
    ) as pair:
        assert pair.server.ipv4_pasv_forced_response_address == ipv4_address

        if ipv4_used():
            # The connection should fail here because the server starts to listen for
            # the passive connections on the host (IPv4 address) that is used
            # by the control channel. In reality, if the server is behind NAT,
            # the server is reached with the defined external IPv4 address,
            # i.e. we can check that the connection to
            # pair.server.ipv4_pasv_forced_response_address failed to know that
            # the server returned correct external IP
            # ...
            # but we can't use check like this:
            # with pytest.raises(OSError):
            #     await pair.client.get_passive_connection(commands=["pasv"])
            # because there is no such ipv4 which will be non-routable, so
            # we can only check `PASV` response
            ip, _ = await pair.client._do_pasv()
            assert ip == ipv4_address

        # With epsv the connection should open as that does not use the
        # external IPv4 address but just tells the client the port to connect
        # to
        await pair.client.get_passive_connection(commands=["epsv"])


@pytest.mark.parametrize("method", ["epsv", "pasv"])
@pytest.mark.asyncio
async def test_pasv_connection_no_free_port(
    pair_factory,
    Server,
    expect_codes_in_exception,
    method,
):
    s = Server(data_ports=[])
    async with pair_factory(None, s, do_quit=False, host="127.0.0.1") as pair:
        assert pair.server.available_data_ports.qsize() == 0
        with expect_codes_in_exception("421"):
            await pair.client.get_passive_connection(commands=[method])


@pytest.mark.asyncio
async def test_pasv_connection_busy_port(
    pair_factory,
    Server,
    unused_tcp_port_factory,
):
    ports = [unused_tcp_port_factory(), unused_tcp_port_factory()]
    async with pair_factory(None, Server(data_ports=ports)) as pair:
        conflicting_server = await asyncio.start_server(
            lambda r, w: w.close(),
            host=pair.server.server_host,
            port=ports[0],
        )
        r, w = await pair.client.get_passive_connection()
        host, port, *_ = w.transport.get_extra_info("peername")
        assert port == ports[1]
        assert pair.server.available_data_ports.qsize() == 1
    conflicting_server.close()
    await conflicting_server.wait_closed()


@pytest.mark.asyncio
async def test_pasv_connection_busy_port2(
    pair_factory,
    Server,
    unused_tcp_port_factory,
    expect_codes_in_exception,
):
    ports = [unused_tcp_port_factory()]
    s = Server(data_ports=ports)
    async with pair_factory(None, s, do_quit=False) as pair:
        conflicting_server = await asyncio.start_server(
            lambda r, w: w.close(),
            host=pair.server.server_host,
            port=ports[0],
        )
        with expect_codes_in_exception("421"):
            await pair.client.get_passive_connection()
    conflicting_server.close()
    await conflicting_server.wait_closed()


@pytest.mark.asyncio
async def test_server_shutdown(pair_factory):
    async with pair_factory(do_quit=False) as pair:
        await pair.client.list()
        await pair.server.close()
        with pytest.raises(ConnectionResetError):
            await pair.client.list()


@pytest.mark.asyncio
async def test_client_session_context_manager(pair_factory):
    async with pair_factory(connected=False) as pair:
        async with aioftp.Client.context(*pair.server.address) as client:
            await client.list()


@pytest.mark.asyncio
async def test_long_login_sequence_fail(
    pair_factory,
    expect_codes_in_exception,
):
    class CustomServer(aioftp.Server):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.commands_mapping["acct"] = self.acct

        async def user(self, connection, rest):
            connection.response("331")
            return True

        async def pass_(self, connection, rest):
            connection.response("332")
            return True

        async def acct(self, connection, rest):
            connection.response("333")
            return True

    factory = pair_factory(
        logged=False,
        server_factory=CustomServer,
        do_quit=False,
    )
    async with factory as pair:
        with expect_codes_in_exception("333"):
            await pair.client.login()


@pytest.mark.asyncio
async def test_bad_sublines_seq(pair_factory, expect_codes_in_exception):
    class CustomServer(aioftp.Server):
        async def write_response(self, stream, code, lines="", list=False):
            import functools

            lines = aioftp.wrap_with_container(lines)
            write = functools.partial(self.write_line, stream)
            *body, tail = lines
            for line in body:
                await write(code + "-" + line)
            await write(str(int(code) + 1) + "-" + tail)
            await write(code + " " + tail)

    factory = pair_factory(connected=False, server_factory=CustomServer)
    async with factory as pair:
        with expect_codes_in_exception("220"):
            await pair.client.connect(
                pair.server.server_host,
                pair.server.server_port,
            )
            await pair.client.login()
