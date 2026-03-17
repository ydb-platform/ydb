from datetime import datetime

import pytest
import xmltodict
from aiohttp_xmlrpc.common import Binary, py2xml, xml2py
from lxml import etree


CASES = [
    (Binary("you can't read this!".encode()), "<base64>eW91IGNhbid0IHJlYWQgdGhpcyE=</base64>"),

    (-12.53, "<double>-12.53</double>"),

    ("Hello world!", "<string>Hello world!</string>"),

    (
        datetime(year=1998, month=7, day=17, hour=14, minute=8, second=55),
        "<dateTime.iso8601>19980717T14:08:55</dateTime.iso8601>",
    ),

    (42, "<i4>42</i4>"),

    (True, "<boolean>1</boolean>"),

    (False, "<boolean>0</boolean>"),

    (None, "<nil/>"),

    ("", "<string/>"),

    (
        [1404, "Something here", 1],
        (
            "<array>"
                "<data>"
                    "<value>"
                        "<i4>1404</i4>"
                    "</value>"
                    "<value>"
                        "<string>Something here</string>"
                    "</value>"
                    "<value>"
                        "<i4>1</i4>"
                    "</value>"
                "</data>"
            "</array>"
        ),
    ),

    (
        {"foo": 1},
        (
            "<struct>"
              "<member>"
                "<name>foo</name>"
                "<value><i4>1</i4></value>"
              "</member>"
            "</struct>"
        ),
    ),

    (
        [[1, "a"]],
        (
            "<array>"
                "<data>"
                    "<value>"
                        "<array>"
                            "<data>"
                                "<value>"
                                    "<i4>1</i4>"
                                "</value>"
                                "<value>"
                                    "<string>a</string>"
                                "</value>"
                            "</data>"
                        "</array>"
                    "</value>"
                "</data>"
            "</array>"
        ),
    ),
]

CASES_COMPAT = [
    ("Hello world!", "<value>Hello world!</value>"),
    (
        [[1, "a"]],
        (
            "<value>"
                "<array>"
                    "<data>"
                        "<value>"
                            "<array>"
                                "<data>"
                                    "<value>"
                                        "<i4>1</i4>"
                                    "</value>"
                                    "<value>"
                                        "a"
                                    "</value>"
                                "</data>"
                            "</array>"
                        "</value>"
                    "</data>"
                "</array>"
            "</value>"
        ),
    ),
    (
        {"foo": "bar"},
        (
            "<value>"
                "<struct>"
                    "<member>"
                        "<name>foo</name>"
                        "<value>bar</value>"
                    "</member>"
                "</struct>"
            "</value>"
        ),
    ),
]


def normalise_dict(d):
    """
    Recursively convert dict-like object (eg OrderedDict) into plain dict.
    Sorts list values.
    """
    out = {}
    for k, v in dict(d).items():
        if hasattr(v, "iteritems"):
            out[k] = normalise_dict(v)
        elif isinstance(v, list):
            out[k] = []
            for item in sorted(v):
                if hasattr(item, "iteritems"):
                    out[k].append(normalise_dict(item))
                else:
                    out[k].append(item)
        else:
            out[k] = v
    return out


@pytest.mark.parametrize("expected,data", CASES + CASES_COMPAT)
def test_xml2py(expected, data):
    data = etree.fromstring(data)
    result = xml2py(data)
    assert result == expected


@pytest.mark.parametrize("data,expected", CASES)
def test_py2xml(data, expected):
    a = py2xml(data)
    b = expected

    if not isinstance(a, str):
        a = etree.tostring(a, encoding="utf-8")
    if not isinstance(b, str):
        b = etree.tostring(b, encoding="utf-8")

    _a = normalise_dict(xmltodict.parse(a))
    _b = normalise_dict(xmltodict.parse(b))

    assert _a == _b, "\n %s \n not equal \n %s" % (a.decode(), b)
