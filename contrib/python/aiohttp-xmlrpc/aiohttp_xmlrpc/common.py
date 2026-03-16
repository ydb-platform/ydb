import asyncio
import base64
import logging
import os
import importlib.resources
from builtins import ValueError
from datetime import datetime
from functools import singledispatch, wraps
from types import GeneratorType

from lxml import etree


NoneType = type(None)

log = logging.getLogger(__name__)

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

TIME_FORMAT = "%Y%m%dT%H:%M:%S"
TIME_FORMATS = [TIME_FORMAT, "%Y%m%dT%H%M%S"]
PY2XML_TYPES = {}
XML2PY_TYPES = {}

schema = etree.RelaxNG(file=importlib.resources.open_binary(__package__, "xmlrpc.rng"))


class Binary(bytes):
    @classmethod
    def fromstring(cls, data):
        if data is None:
            return b""

        return cls(base64.b64decode(data))


@singledispatch
def py2xml(value):
    raise TypeError(("Can't serialise type: {0}."
                     " Add type {0} via decorator "
                     "@py2xml.register({0}) ").format(type(value)))


@py2xml.register(bytes)
def _(value):
    value = value.decode()
    el = etree.Element("string")
    el.text = str(value)
    return el


@py2xml.register(str)
def _(value):
    el = etree.Element("string")
    el.text = str(value)
    return el


@py2xml.register(float)
def _(value):
    el = etree.Element("double")
    el.text = str(value)
    return el


@py2xml.register(datetime)
def _(value):
    el = etree.Element("dateTime.iso8601")
    el.text = value.strftime(TIME_FORMAT)
    return el


@py2xml.register(int)
def _(value):
    if -2147483648 < value < 2147483647:
        el = etree.Element("i4")
    else:
        el = etree.Element("double")

    el.text = str(value)
    return el


@py2xml.register(Binary)
def _(value):
    el = etree.Element("base64")
    el.text = base64.b64encode(value)
    return el


@py2xml.register(bool)
def _(value):
    el = etree.Element("boolean")
    el.text = "1" if value else "0"
    return el


@py2xml.register(NoneType)
def _(value):
    return etree.Element("nil")


@py2xml.register(list)
@py2xml.register(tuple)
@py2xml.register(set)
@py2xml.register(frozenset)
@py2xml.register(GeneratorType)
def _(x):
    array = etree.Element("array")
    data = etree.Element("data")
    array.append(data)

    for i in x:
        el = etree.Element("value")
        el.append(py2xml(i))
        data.append(el)

    return array


@py2xml.register(dict)
def _(x):
    struct = etree.Element("struct")

    for key, value in x.items():
        member = etree.Element("member")
        struct.append(member)

        key_el = etree.Element("name")
        key_el.text = str(key)
        member.append(key_el)

        value_el = etree.Element("value")
        value_el.append(py2xml(value))
        member.append(value_el)

    return struct


def str_to_time(x):
    for format in TIME_FORMATS:
        try:
            return datetime.strptime(x.text, format)
        except ValueError:
            pass

    raise ValueError("It's impossible to parse dataetime with formats %s", TIME_FORMATS)


def xml2py(value):
    def xml2struct(p):
        return dict(
            map(
                lambda x: (x[0].text, x[1]),
                zip(
                    p.xpath("./member/name"),
                    map(xml2py, p.xpath("./member/value")),
                ),
            ),
        )

    def xml2array(p):
        return list(map(xml2py, p.xpath("./data/value")))

    def unwrap_value(p):
        try:
            value = next(p.iterchildren())
        except StopIteration:
            value = p.text or ''
        return xml2py(value)

    XML2PY_TYPES.update({
        "string": lambda x: str(x.text or "").strip(),
        "struct": xml2struct,
        "array": xml2array,
        "base64": lambda x: Binary.fromstring(x.text),
        "boolean": lambda x: bool(int(x.text)),
        "dateTime.iso8601": lambda x: str_to_time(x),
        "double": lambda x: float(x.text),
        "integer": lambda x: int(x.text),
        "int": lambda x: int(x.text),
        "i4": lambda x: int(x.text),
        "nil": lambda x: None,
        "value": unwrap_value,
    })

    if isinstance(value, str):
        return value.strip()

    return XML2PY_TYPES.get(value.tag)(value)


def awaitable(func):
    # Avoid python 3.8+ warning
    if asyncio.iscoroutinefunction(func):
        return func

    async def awaiter(obj):
        return obj

    @wraps(func)
    def wrap(*args, **kwargs):
        result = func(*args, **kwargs)

        if hasattr(result, "__await__"):
            return result
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            return result

        return awaiter(result)

    return wrap


__all__ = ("py2xml", "xml2py", "schema", "awaitable")
