# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from abc import ABCMeta, abstractmethod

from six import add_metaclass

from efc import Lexer, Parser
from efc.interfaces.cache import CacheManager
from efc.interfaces.errors import NamedRangeNotFound


@add_metaclass(ABCMeta)
class BaseExcelInterface(object):
    """
    Base class to working with excel document
    """

    def __init__(self, use_cache=False, lexer=Lexer, parser=Parser):
        self._cache_manager = CacheManager() if use_cache else None

        self.lexer = lexer()
        self.parser = parser()

    def _build_rpn(self, formula, ws_name=None):
        tokens_line = self.lexer.parse(formula)
        return self.parser.to_rpn(tokens_line, ws_name=ws_name, source=self)

    def _calc_formula(self, formula, ws_name=None):
        """
        Calculate formula
        :type formula: str
        :type ws_name: str
        """
        rpn = self._build_rpn(formula, ws_name)
        return rpn.calc(ws_name, self)

    @property
    def _caches(self):
        return self._cache_manager

    def clear_cache(self):
        """Clear all caches"""
        self._caches.clear()

    @abstractmethod
    def _cell_to_value(self, row, column, ws_name):
        """
        :type row: int
        :type column: int
        :type ws_name: basestring
        :rtype: list
        """

    @abstractmethod
    def _get_named_range_formula(self, name, ws_name):
        """
        Should raise NamedRangeNotFound if named range not found
        :type name: basestring
        :type ws_name: basestring
        :rtype: basestring
        """

    @abstractmethod
    def _max_row(self, ws_name):
        pass

    @abstractmethod
    def _min_row(self, ws_name):
        pass

    @abstractmethod
    def _max_column(self, ws_name):
        pass

    @abstractmethod
    def _min_column(self, ws_name):
        pass

    @abstractmethod
    def _has_worksheet(self, ws_name):
        pass

    def _named_range_to_cells(self, name, ws_name):
        f = self._get_named_range_formula(name, ws_name)
        return self._calc_formula(f, ws_name)

    def _has_named_range(self, name, ws_name):
        try:
            self._get_named_range_formula(name, ws_name)
        except NamedRangeNotFound:
            return False
        else:
            return True
