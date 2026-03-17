#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import Any

from elementpath.helpers import collapse_white_spaces, Patterns, LazyPattern
from .any_types import AnyAtomicType

__all__ = ['NormalizedString', 'XsdToken', 'Name', 'NCName',
           'NMToken', 'Id', 'Idref', 'Language', 'Entity']


class NormalizedString(str, AnyAtomicType):
    name = 'normalizedString'
    pattern = LazyPattern('^[^\t\r]*$')

    def __new__(cls, obj: Any) -> 'NormalizedString':
        try:
            return super().__new__(cls, Patterns.normalize.sub(' ', obj))
        except TypeError:
            return super().__new__(cls, obj)

    def __init__(self, obj: Any) -> None:
        """
        :param obj: a <class 'str'> instance.
        """
        str.__init__(self)


class XsdToken(NormalizedString):
    name = 'token'
    pattern = LazyPattern(r'^[\S\xa0]*(?: [\S\xa0]+)*$')

    def __new__(cls, value: Any) -> 'XsdToken':
        if not isinstance(value, str):
            value = str(value)
        else:
            value = collapse_white_spaces(value)

        match = cls.pattern.match(value)
        if match is None:
            raise ValueError('invalid value {!r} for xs:{}'.format(value, cls.name))
        return super(NormalizedString, cls).__new__(cls, value)  # noqa


class Language(XsdToken):
    name = 'language'
    pattern = LazyPattern(r'^[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8})*$')

    def __new__(cls, value: Any) -> 'Language':
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        elif not isinstance(value, str):
            value = str(value)
        else:
            value = collapse_white_spaces(value)

        match = cls.pattern.match(value)
        if match is None:
            raise ValueError('invalid value {!r} for xs:{}'.format(value, cls.name))
        return super(NormalizedString, cls).__new__(cls, value)  # noqa


class Name(XsdToken):
    name = 'Name'
    pattern = LazyPattern(r'^(?:[^\d\W]|:)[\w.\-:\u00B7\u0300-\u036F\u203F\u2040]*$')


class NCName(Name):
    name = 'NCName'
    pattern = LazyPattern(r'^[^\d\W][\w.\-\u00B7\u0300-\u036F\u203F\u2040]*$')


class Id(NCName):
    name = 'ID'


class Idref(NCName):
    name = 'IDREF'


class Entity(NCName):
    name = 'ENTITY'


class NMToken(XsdToken):
    name = 'NMTOKEN'
    pattern = LazyPattern(r'^[\w.\-:\u00B7\u0300-\u036F\u203F\u2040]+$')
