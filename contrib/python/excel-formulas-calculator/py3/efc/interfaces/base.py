# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from abc import ABCMeta, abstractmethod
from functools import partial

from six import add_metaclass

from efc import Lexer, Parser
from efc.interfaces.cache import CacheManager
from efc.interfaces.errors import NamedRangeNotFound
from efc.rpn_builder.parser.operands import CellAddress, HyperlinkOperand, RPNOperand, SingleCellOperand


class CellInfo:
    def __init__(self, value, formula=None):
        self.value = value
        self.formula = formula


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

    def _get_cell_formula_hyperlink(self, address):
        """
        :type address: CellAddress
        :rtype: str | None
        """
        if not self._caches:
            cell_info = self._get_cell_info(address)
            v = self._calc_extended_value(address, cell_info)
            return v[1].link if isinstance(v[1], HyperlinkOperand) else None
        else:
            self._cell_to_value(address)
            return self._caches['hyperlinks'].get(address)

    @abstractmethod
    def _get_cell_info(self, address):
        """
        :type address: CellAddress
        :rtype: CellInfo
        """
        pass

    def _calc_extended_value(self, cell_addr, cell_info):
        """
        :type cell_addr: CellAddress
        :type cell_info: CellInfo
        """
        last_cell_address = cell_addr
        calc = partial(self._build_rpn(cell_info.formula, cell_addr.ws_name).calc, cell_addr.ws_name, self)
        while True:
            partial_result = calc()
            if isinstance(partial_result, SingleCellOperand):
                value, last_cell_address = self._cell_to_value(partial_result.cell_address)
                break
            elif isinstance(partial_result, RPNOperand):
                calc = partial(partial_result.rpn.calc,
                               ws_name=partial_result.ws_name,
                               source=partial_result.source)
            else:
                value = partial_result.value
                break
        return value, partial_result, last_cell_address
    
    def _store_to_cache(self, cell_addr, value, partial_result, last_cell_address):
        if isinstance(partial_result, HyperlinkOperand):
            self._caches['hyperlinks'][cell_addr] = partial_result.link
        self._caches['cells'][cell_addr] = (value, last_cell_address)
        
    def _get_or_calc_extended_value(self, cell_addr, cell_info):
        """
        :type cell_addr: CellAddress
        :type cell_info: CellInfo
        """
        if self._caches:
            if cell_addr not in self._caches['cells']:
                v, pr, lca = self._calc_extended_value(cell_addr, cell_info)
                self._store_to_cache(cell_addr, v, pr, lca)
            return self._caches['cells'][cell_addr]
        else:
            v, _, lca = self._calc_extended_value(cell_addr, cell_info)
            return v, lca

    def _cell_to_value(self, cell_addr):
        """
        rtype: tuple[Any, CellAddress]
        """
        cell_info = self._get_cell_info(cell_addr)  # type: CellInfo

        if cell_info.formula is None:
            return cell_info.value, cell_addr
        else:
            return self._get_or_calc_extended_value(cell_addr, cell_info)

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
