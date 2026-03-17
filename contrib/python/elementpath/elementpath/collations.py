#
# Copyright (c), 2022-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import locale
import threading
from contextlib import AbstractContextManager
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urljoin, urlsplit

from elementpath.exceptions import xpath_error

if TYPE_CHECKING:
    from .xpath_tokens import XPathToken
    context_class_base = AbstractContextManager[Any]
else:
    context_class_base = AbstractContextManager

UNICODE_COLLATION_BASE_URI = "http://www.w3.org/2013/collation/UCA"

UNICODE_CODEPOINT_COLLATION = \
    "http://www.w3.org/2005/xpath-functions/collation/codepoint"

HTML_ASCII_CASE_INSENSITIVE_COLLATION = \
    "http://www.w3.org/2005/xpath-functions/collation/html-ascii-case-insensitive"

XQUERY_TEST_SUITE_CASEBLIND_COLLATION = \
    "http://www.w3.org/2010/09/qt-fots-catalog/collation/caseblind"

_locale_collate_lock = threading.Lock()


def get_locale_category(category: int) -> str:
    """
    Gets the current value of a locale category. A replacement
    of locale.getdefaultlocale(), deprecated since Python 3.11.
    """
    _locale = locale.setlocale(category, None)
    if _locale == 'C':
        # locale category does not seem to be configured, so get the user
        # preferred locale and then restore the  previous state
        _locale = locale.setlocale(category, '')
        locale.setlocale(category, 'C')

    return _locale


def unicode_codepoint_strcoll(s1: str, s2: str) -> int:
    return 0 if s1 == s2 else -1 if s1 < s2 else 1


def unicode_codepoint_strxfrm(s: str) -> str:
    return s


def case_insensitive_strcoll(s1: str, s2: str) -> int:
    if s1.casefold() == s2.casefold():
        return 0
    elif s1.casefold() < s2.casefold():
        return -1
    else:
        return 1


def case_insensitive_strxfrm(s: str) -> str:
    return s.casefold()


class CollationManager(context_class_base):
    """
    Context Manager for collations. Provide helper operators as methods.
    """
    lc_collate: Union[None, str, tuple[Optional[str], Optional[str]]]
    fallback: bool = False
    _current_lc_collate: Optional[tuple[Optional[str], Optional[str]]] = None

    def __init__(self,
                 collation: Optional[str],
                 token: Optional['XPathToken'] = None) -> None:
        self.collation = collation
        self.token = token
        self.strcoll = locale.strcoll
        self.strxfrm = locale.strxfrm

        if collation is None:
            msg = 'collation cannot be an empty sequence'
            raise xpath_error('XPTY0004', msg, self.token)
        elif not urlsplit(collation).scheme and token is not None:
            # Collation is a relative URI: try to complete with the static base URI
            base_uri = token.parser.base_uri
            if base_uri:
                collation = urljoin(base_uri, collation)

        if collation == UNICODE_CODEPOINT_COLLATION:
            self.lc_collate = None
            self.strcoll = unicode_codepoint_strcoll
            self.strxfrm = unicode_codepoint_strxfrm
        elif collation == HTML_ASCII_CASE_INSENSITIVE_COLLATION:
            self.lc_collate = None
            self.strcoll = case_insensitive_strcoll
            self.strxfrm = case_insensitive_strxfrm
        elif collation == XQUERY_TEST_SUITE_CASEBLIND_COLLATION:
            self.lc_collate = None
            self.strcoll = case_insensitive_strcoll
            self.strxfrm = case_insensitive_strxfrm
        elif collation.startswith(UNICODE_COLLATION_BASE_URI):
            self.lc_collate = 'en_US.UTF-8'
            self.fallback = True
            for param in urlsplit(collation).query.split(';'):
                assert isinstance(param, str)
                if param.startswith('lang='):
                    # Language code: should be a string in lexical space of xs:language,
                    # but in implementations '_' can be used instead of hyphens and '.'
                    # is used to provide the encoding. Use UTF-8 as default encoding.
                    lang = param[5:]
                    self.lc_collate = lang if '.' in lang else (lang, 'UTF-8')
                elif param.startswith('fallback='):
                    if param.endswith('yes'):
                        self.fallback = True
                    elif param.endswith('no'):
                        self.fallback = False
        else:
            # Other compatible collations locale lib specs (e.g.: it_IT.UTF-8)
            self.lc_collate = collation

    def __enter__(self) -> 'CollationManager':
        if self.lc_collate is not None:
            # Only one locale set can be used at a time
            _locale_collate_lock.acquire()
            self._current_lc_collate = locale.getlocale(locale.LC_COLLATE)

            try:
                locale.setlocale(locale.LC_COLLATE, self.lc_collate)
            except locale.Error:
                if not self.fallback:
                    self._current_lc_collate = None
                    _locale_collate_lock.release()

                    msg = f"Unsupported collation {self.collation!r}"
                    raise xpath_error('FOCH0002', msg, self.token) from None

                locale.setlocale(locale.LC_COLLATE, 'en_US.UTF-8')

        return self

    def __exit__(self, exc_type: Optional[type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        if self._current_lc_collate is not None:
            locale.setlocale(locale.LC_COLLATE, self._current_lc_collate)
            self._current_lc_collate = None
            _locale_collate_lock.release()

    def eq(self, a: Any, b: Any) -> bool:
        if not isinstance(a, str) or not isinstance(b, str):
            return bool(a == b)
        return self.strcoll(a, b) == 0

    def ne(self, a: Any, b: Any) -> bool:
        if not isinstance(a, str) or not isinstance(b, str):
            return bool(a != b)
        return self.strcoll(a, b) != 0

    def contains(self, a: str, b: str) -> bool:
        return self.strxfrm(b) in self.strxfrm(a)

    def find(self, a: str, b: str) -> int:
        return self.strxfrm(a).find(self.strxfrm(b))

    def startswith(self, a: str, b: str) -> bool:
        return self.strxfrm(a).startswith(self.strxfrm(b))

    def endswith(self, a: str, b: str) -> bool:
        return self.strxfrm(a).endswith(self.strxfrm(b))
