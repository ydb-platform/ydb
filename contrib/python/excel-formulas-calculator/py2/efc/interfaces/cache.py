# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from abc import ABCMeta, abstractmethod

from six import add_metaclass


@add_metaclass(ABCMeta)
class BaseCache(object):
    def __init__(self):
        self._items = {}

    def clear(self):
        self._items.clear()

    @abstractmethod
    def remove_cell(self, ws_name, row, column):
        pass

    def __getitem__(self, item):
        return self._items[item]

    def get(self, key, default=None):
        try:
            return self._items[key]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        self._items[key] = value

    def __contains__(self, item):
        return item in self._items


class SingleCellCache(BaseCache):
    def remove_cell(self, ws_name, row, column):
        for row_fixed, column_fixed in ((False, False), (False, True), (True, False), (True, True)):
            key = (ws_name, row, column, row_fixed, column_fixed)
            if key in self._items:
                del self._items[key]


class RangeCache(BaseCache):
    def remove_cell(self, ws_name, row, column):
        for key in list(self._items):
            if ws_name == key[0]:
                if key[1] is None:
                    if key[2] >= column >= key[4]:
                        del self._items[key]
                elif key[2] is None:
                    if key[1] >= row >= key[3]:
                        del self._items[key]
                elif key[1] >= row >= key[3] and key[2] >= column >= key[4]:
                    del self._items[key]


class CacheManager(object):
    CACHE_TYPES = {
        'single': SingleCellCache,  # cache SingleCellOperand instances
        'cells': SingleCellCache,  # cache calculated Cells
        'range': RangeCache,  # cache CellRangeOperand instances
        'ifs': RangeCache,  # cache IFS expressions for ranges
    }

    def __init__(self):
        self._caches = {}

    def __getitem__(self, item):
        if item not in self._caches:
            self._caches[item] = self.CACHE_TYPES[item]()
        return self._caches[item]

    def clear(self):
        self._caches.clear()

    def remove_cell(self, ws_name, row, column):
        for cache in self._caches.values():
            cache.remove_cell(ws_name, row, column)
