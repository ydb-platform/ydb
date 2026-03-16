import asyncio
import datetime

import pytest
from aiohttp import web
from aiohttp_xmlrpc import handler
from aiohttp_xmlrpc.exceptions import ApplicationError
from lxml import etree
from lxml.builder import E

from aiohttp_xmlrpc.handler import rename

pytest_plugins = (
    "aiohttp.pytest_plugin",
    "aiohttp_xmlrpc.pytest_plugin",
)


class XMLRPCMain(handler.XMLRPCView):

    def rpc_test(self):
        return None

    def rpc_args(self, *args):
        return len(args)

    def rpc_kwargs(self, **kwargs):
        return len(kwargs)

    def rpc_args_kwargs(self, *args, **kwargs):
        return len(args) + len(kwargs)

    def rpc_exception(self):
        raise Exception("YEEEEEE!!!")

    def rpc_strings(self, s1, s2):
        return s1 == s2

    def rpc_datetime(self, test_datetime_1, test_datetime_2):
        return test_datetime_1, test_datetime_2

    def rpc_future(self):
        f = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.call_soon(f.set_result, 42)
        return f

    def rpc_dict_args(self, a, b, d):
        return (a, b, d)

    def rpc_dict_kwargs(self, d, **kw):
        return (d, kw)

    def rpc_dict_kw_only_args(self, d, *, foo, **kw):
        return (d, foo, kw)

    @rename("method_with.new_name")
    def rpc_renamed(self):
        return "renamed_function"


class XMLRPCChild(XMLRPCMain):

    def rpc_child(self):
        return 42

    @rename("child.test")
    def rpc_child_nested_method(self):
        return "My name has the nested format and I am in child class"


def create_app(loop):
    app = web.Application()
    app.router.add_route("*", "/", XMLRPCMain)
    app.router.add_route("*", "/clone", XMLRPCChild)
    return app


@pytest.fixture
def client(loop, aiohttp_xmlrpc_client):
    return loop.run_until_complete(aiohttp_xmlrpc_client(create_app))


async def test_1_test(client):
    result = await client.test()
    assert result is None


async def test_2_args(client):
    result = await client.args(1, 2, 3, 4, 5)
    assert result == 5


async def test_3_kwargs(client):
    result = await client.kwargs(foo=1, bar=2)
    assert result == 2


async def test_4_kwargs(client):
    result = await client.args_kwargs(1, 2, 3, 4, 5, foo=1, bar=2)
    assert result == 7


async def test_5_exception(client):
    with pytest.raises(Exception):
        await client.exception()


async def test_6_unknown_method(client):
    with pytest.raises(ApplicationError):
        await client.unknown_method()


async def test_7_strings(aiohttp_client):
    request = E.methodCall(
        E.methodName("strings"),
        E.params(
            E.param(
                E.value("Some string"),
            ),
            E.param(
                E.value(
                    E.string("Some string"),
                ),
            ),
        ),
    )
    client = await aiohttp_client(create_app)

    async with client.post(
        "/",
        data=etree.tostring(request, xml_declaration=True),
        headers={"Content-Type": "text/xml"},
    ) as resp:
        assert resp.status == 200

        root = etree.fromstring((await resp.read()))
    assert root.xpath("//value/boolean/text()")[0] == "1"


async def test_8_strings_pretty(aiohttp_client):
    request = E.methodCall(
        E.methodName("strings"),
        E.params(
            E.param(
                E.value("Some string"),
            ),
            E.param(
                E.value(
                    E.string("Some string"),
                ),
            ),
        ),
    )
    client = await aiohttp_client(create_app)

    async with await client.post(
        "/",
        data=etree.tostring(request, xml_declaration=True, pretty_print=True),
        headers={"Content-Type": "text/xml"},
    ) as resp:
        assert resp.status == 200

        root = etree.fromstring((await resp.read()))
    assert root.xpath("//value/boolean/text()")[0] == "1"


async def test_9_datetime(aiohttp_client):
    resp_date = datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S")
    test_date = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    request = E.methodCall(
        E.methodName("datetime"),
        E.params(
            E.param(
                E.value(
                    E("dateTime.iso8601", test_date),
                ),
            ),
            E.param(
                E.value(
                    E("dateTime.iso8601", resp_date),
                ),
            ),
        ),
    )
    client = await aiohttp_client(create_app)

    async with client.post(
        "/",
        data=etree.tostring(request, xml_declaration=True, pretty_print=True),
        headers={"Content-Type": "text/xml"},
    ) as resp:
        assert resp.status == 200

        root = etree.fromstring((await resp.read()))
    assert root.xpath("//value/dateTime.iso8601/text()")[0] == resp_date
    assert root.xpath("//value/dateTime.iso8601/text()")[1] == resp_date


async def test_10_future(client):
    result = await client.future()
    assert result == 42


async def test_11_dict_args(client):
    result = await client.dict_args(41, 42, {"foo": "bar"})
    assert result == [41, 42, {"foo": "bar"}]

    result = await client.dict_kwargs({"foo": "bar"}, spam="egg")
    assert result == [{"foo": "bar"}, {"spam": "egg"}]


async def test_12_inherited(aiohttp_xmlrpc_client):
    client = await aiohttp_xmlrpc_client(create_app, path="/clone")

    result = await client.future()
    assert result == 42

    result = await client.child()
    assert result == 42


async def test_13_kw_only_args(client):
    result = await client.dict_kw_only_args(
        {"foo": "bar"}, foo=32, spam="egg"
    )
    assert result == [{"foo": "bar"}, 32, {"spam": "egg"}]


async def test_14_method_renaming(client):
    result = await client.method_with.new_name()
    assert result == "renamed_function"


async def test_15_nested_method_in_child(aiohttp_xmlrpc_client):
    client = await aiohttp_xmlrpc_client(create_app, path="/clone")

    result = await client.child.test()
    assert result == "My name has the nested format and I am in child class"
