# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals


class MetaSingleCellOperandCache(type):
    def __call__(cls, row, column, row_fixed=False, column_fixed=False, ws_name=None, source=None):
        if source is not None and ws_name is not None and source._caches:
            cache = source._caches['single']
            key = (ws_name, row, column, row_fixed, column_fixed)

            try:
                return cache[key]
            except KeyError:
                value = cache[key] = super(MetaSingleCellOperandCache, cls).__call__(row, column, row_fixed=row_fixed,
                                                                                     column_fixed=column_fixed,
                                                                                     ws_name=ws_name,
                                                                                     source=source)
                return value
        else:
            return super(MetaSingleCellOperandCache, cls).__call__(row, column, row_fixed=row_fixed,
                                                                   column_fixed=column_fixed, ws_name=ws_name,
                                                                   source=source)


class MetaCellRangeOperandCache(type):
    def __call__(cls, row1, column1, row2, column2,
                 row1_fixed=False, column1_fixed=False, row2_fixed=False, column2_fixed=False,
                 ws_name=None, source=None):
        if source is not None and ws_name is not None and source._caches:
            cache = source._caches['range']
            key = (ws_name, row1, column1, row2, column2, row1_fixed, column1_fixed, row2_fixed, column2_fixed)

            try:
                return cache[key]
            except KeyError:
                value = cache[key] = super(MetaCellRangeOperandCache, cls).__call__(
                    row1, column1, row2, column2, row1_fixed, column1_fixed, row2_fixed, column2_fixed, ws_name, source
                )
                return value
        else:
            return super(MetaCellRangeOperandCache, cls).__call__(row1, column1, row2, column2, row1_fixed,
                                                                  column1_fixed, row2_fixed, column2_fixed, ws_name,
                                                                  source)
