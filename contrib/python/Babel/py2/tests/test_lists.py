# coding=utf-8
import pytest

from babel import lists


def test_format_list():
    for list, locale, expected in [
        ([], 'en', ''),
        ([u'string'], 'en', u'string'),
        (['string1', 'string2'], 'en', u'string1 and string2'),
        (['string1', 'string2', 'string3'], 'en', u'string1, string2, and string3'),
        (['string1', 'string2', 'string3'], 'zh', u'string1、string2和string3'),
        (['string1', 'string2', 'string3', 'string4'], 'ne', u'string1,string2, string3 र string4'),
    ]:
        assert lists.format_list(list, locale=locale) == expected


def test_format_list_error():
    with pytest.raises(ValueError):
        lists.format_list(['a', 'b', 'c'], style='orange', locale='en')
