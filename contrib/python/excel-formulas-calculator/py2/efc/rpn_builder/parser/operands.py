# -*- coding: utf8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from collections import defaultdict

from six import add_metaclass, itervalues, python_2_unicode_compatible, text_type
from six.moves import range

from efc import settings
from efc.base.errors import BaseEFCException
from efc.rpn_builder.parser.metaclasses import MetaCellRangeOperandCache, MetaSingleCellOperandCache
from efc.utils import cached_property, col_index_to_str, digit, u

__all__ = (
    'Operand', 'ErrorOperand', 'ValueErrorOperand', 'WorksheetNotExist',
    'ZeroDivisionErrorOperand', 'SimpleOperand', 'SingleCellOperand',
    'CellSetOperand', 'SimpleSetOperand', 'NamedRangeOperand', 'CellRangeOperand',
    'FunctionNotSupported', 'NotFoundErrorOperand', 'RPNOperand', 'OperandLikeObject', 'OffsetMixin',
    'SetOperand', 'BadReference', 'ValueNotAvailable', 'EmptyOperand', 'NamedRangeNotExist', 'NumErrorOperand',
)


class OperandLikeObject(object):
    def __init__(self, ws_name=None, source=None, *args, **kwargs):
        super(OperandLikeObject, self).__init__(*args, **kwargs)
        self.ws_name = ws_name
        self.source = source


@python_2_unicode_compatible
class Operand(OperandLikeObject):
    value = None

    @cached_property
    def digit(self):
        """Digit type"""
        return digit(self.value)

    @cached_property
    def string(self):
        """String type"""
        value = self.value

        if isinstance(value, bool):
            return text_type(value).upper()
        elif isinstance(value, float):
            if value % 1 == 0:
                return text_type(int(value))
            else:
                return text_type(value).replace('.', settings.FLOAT_DELIMITER)
        elif value is not None:
            return u(value)
        else:
            return ''

    @cached_property
    def is_blank(self):
        return self.value in {None, ''}

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        return self.string

    def __trunc__(self):
        return self.__int__()


class EmptyOperand(Operand):
    @cached_property
    def value(self):
        return 0

    @cached_property
    def string(self):
        return ''


class BlankOperand(Operand):
    pass


BLANK_OPERAND = BlankOperand()


class ErrorOperand(OperandLikeObject, BaseEFCException):
    code = 300
    msg = 'Unknown error'
    string_value = '#ERROR!'

    def __init__(self, *args, **kwargs):
        self.formula = kwargs.pop('formula', None)
        super(ErrorOperand, self).__init__(*args, **kwargs)

    @property
    def value(self):
        raise self

    @property
    def string(self):
        return self.string_value

    def __getattr__(self, item):
        if item in frozenset(('__notes__',)):
            return super().__getattr__(item)
        raise self


class ValueErrorOperand(ErrorOperand):
    code = 301
    msg = 'Cell value error'
    string_value = '#VALUE!'


class WorksheetNotExist(ErrorOperand):
    code = 302
    msg = 'Worksheet does not exist'
    string_value = '#REF!'


class NamedRangeNotExist(ErrorOperand):
    code = 303
    msg = 'Named range "{name}" does not exist'
    string_value = '#NAME?'

    def __init__(self, name, *args, **kwargs):
        super(NamedRangeNotExist, self).__init__(*args, **kwargs)
        self.name = name


class ZeroDivisionErrorOperand(ErrorOperand):
    code = 304
    msg = 'Zero division'
    string_value = '#DIV/0!'


class NotFoundErrorOperand(ErrorOperand):
    code = 305
    msg = 'Result not found'
    string_value = '#VALUE!'


class FunctionNotSupported(ErrorOperand):
    code = 306
    msg = 'Function "{f_name}" not found among available functions'
    string_value = '#NAME?'

    def __init__(self, f_name, *args, **kwargs):
        super(FunctionNotSupported, self).__init__(*args, **kwargs)
        self.f_name = f_name


class BadReference(ErrorOperand):
    code = 307
    msg = 'Bad reference'
    string_value = '#REF!'


class ValueNotAvailable(ErrorOperand):
    code = 308
    msg = 'Value not available'
    string_value = '#N/A'


class NumErrorOperand(ErrorOperand):
    code = 309
    msg = 'Num error'
    string_value = '#NUM!'


class SimpleOperand(Operand):
    def __init__(self, value, *args, **kwargs):
        super(SimpleOperand, self).__init__(*args, **kwargs)
        self.value = value


class CellsOperand(OperandLikeObject):
    def address_to_value(self):
        raise NotImplementedError

    @cached_property
    def value(self):
        if self.source._has_worksheet(self.ws_name):
            return self.address_to_value()
        else:
            raise WorksheetNotExist(ws_name=self.ws_name)

    def get_iter(self):
        raise NotImplementedError

    @cached_property
    def cached_iterable_items(self):
        return list(self.get_iter())

    def __iter__(self):
        return iter(self.cached_iterable_items)


class OffsetMixin(object):
    def offset(self, row_offset=0, col_offset=0):
        raise NotImplementedError


@add_metaclass(MetaSingleCellOperandCache)
class SingleCellOperand(CellsOperand, Operand, OffsetMixin):
    def __init__(self, row, column, row_fixed=False, column_fixed=False, *args, **kwargs):
        super(SingleCellOperand, self).__init__(*args, **kwargs)
        self.row = row
        self.column = column
        self.row_fixed = row_fixed
        self.column_fixed = column_fixed

    def address_to_value(self):
        return self.source._cell_to_value(self.row, self.column, self.ws_name)

    def get_iter(self):
        yield self

    @property
    def address(self):
        return "'%s'!%s%d" % (self.ws_name, col_index_to_str(self.column), self.row)

    def offset(self, row_offset=0, col_offset=0):
        row = self.row
        column = self.column

        if not self.row_fixed:
            row += row_offset
        if not self.column_fixed:
            column += col_offset

        return SingleCellOperand(row=row, column=column,
                                 row_fixed=self.row_fixed, column_fixed=self.column_fixed,
                                 ws_name=self.ws_name, source=self.source)


class SetOperand(OperandLikeObject):
    operands_type = None

    def __init__(self, *args, **kwargs):
        super(SetOperand, self).__init__(*args, **kwargs)
        self._items = defaultdict(list)

    def check_type(self, items):
        if isinstance(items, list):
            if any(not isinstance(i, self.operands_type) for i in items):
                raise ValueErrorOperand()
        elif not isinstance(items, (self.operands_type, ValueNotAvailable)):
            raise ValueErrorOperand()

    def add_cell(self, item, row=0):
        self.check_type(item)
        self._items[row].append(item)

    def add_many(self, items, row=0):
        self.check_type(items)
        append = self._items[row].append
        for item in items:
            append(item)

    def add_row(self, items):
        self.check_type(items)
        r = max(self._items) + 1 if self._items else 1
        self.add_many(items, r)

    def get_iter(self):
        for r in sorted(self._items):
            for item in self._items[r]:
                yield item

    def __iter__(self):
        return self.get_iter()

    @cached_property
    def value(self):
        return list(self)

    def get_cell(self, row, column):
        try:
            return self._items[row - 1][column - 1]
        except KeyError:
            return BadReference()

    @property
    def rows_count(self):
        return len(self._items)

    @property
    def columns_count(self):
        return max(len(c) for c in itervalues(self._items))


class CellSetOperand(SetOperand):
    operands_type = SingleCellOperand


class SimpleSetOperand(SetOperand):
    operands_type = SimpleOperand


@add_metaclass(MetaCellRangeOperandCache)
class CellRangeOperand(CellsOperand, OffsetMixin):
    def __init__(self, row1, column1, row2, column2,
                 row1_fixed=False, column1_fixed=False, row2_fixed=False, column2_fixed=False,
                 *args, **kwargs):
        super(CellRangeOperand, self).__init__(*args, **kwargs)
        self.row1 = row1
        self.column1 = column1
        self.row2 = row2
        self.column2 = column2

        self.row1_fixed = row1_fixed
        self.column1_fixed = column1_fixed
        self.row2_fixed = row2_fixed
        self.column2_fixed = column2_fixed

    @cached_property
    def min_column(self):
        return self.source._min_column(self.ws_name)

    @cached_property
    def max_column(self):
        return self.source._max_column(self.ws_name)

    @cached_property
    def min_row(self):
        return self.source._min_row(self.ws_name)

    @cached_property
    def max_row(self):
        return self.source._max_row(self.ws_name)

    def _row_cells_generator(self, r, column1, column2):
        for c in range(column1, self.min_column):
            yield BLANK_OPERAND

        for c in range(max(self.min_column, column1), min(column2 + 1, self.max_column + 1)):
            yield SingleCellOperand(r, c, ws_name=self.ws_name, source=self.source)

        for c in range(self.max_column + 1, column2 + 1):
            yield BLANK_OPERAND

    def get_iter(self):
        row1 = self.min_row if self.row1 is None else self.row1
        row2 = self.max_row if self.row2 is None else self.row2
        column1 = self.min_column if self.column1 is None else self.column1
        column2 = self.max_column if self.column2 is None else self.column2

        for _ in range(row1, self.min_row):
            for _ in range(column1, column2 + 1):
                yield BLANK_OPERAND

        for r in range(max(self.min_row, row1), min(row2 + 1, self.max_row + 1)):
            for c in self._row_cells_generator(r, column1, column2):
                yield c

        for r in range(self.max_row + 1, row2 + 1):
            for _ in range(column1, column2 + 1):
                yield BLANK_OPERAND

    def address_to_value(self):
        return self.cached_iterable_items

    @property
    def address(self):
        if self.column1 is not None and self.row1 is not None:
            return "'%s'!%s%d:%s%d" % (self.ws_name,
                                       col_index_to_str(self.column1), self.row1,
                                       col_index_to_str(self.column2), self.row2)
        elif self.column1 is not None:
            return "'%s'!%s:%s" % (self.ws_name,
                                   col_index_to_str(self.column1),
                                   col_index_to_str(self.column2))
        else:
            return "'%s'!%d:%d" % (self.ws_name, self.row1, self.row2)

    def offset(self, row_offset=0, col_offset=0):
        row1 = self.row1
        column1 = self.column1
        row2 = self.row2
        column2 = self.column2

        if not self.row1_fixed and self.row1 is not None:
            row1 += row_offset
        if not self.column1_fixed and self.column1 is not None:
            column1 += col_offset
        if not self.row2_fixed and self.row2 is not None:
            row2 += row_offset
        if not self.column2_fixed and self.column2 is not None:
            column2 += col_offset

        return CellRangeOperand(row1=row1, column1=column1,
                                row2=row2, column2=column2,
                                row1_fixed=self.row1_fixed, column1_fixed=self.column1_fixed,
                                row2_fixed=self.row2_fixed, column2_fixed=self.column2_fixed,
                                ws_name=self.ws_name, source=self.source)

    def get_cell(self, row, column):
        row = (self.row1 or 1) + row - 1
        column = (self.column1 or 1) + column - 1

        if self.row1 is None or self.row1 <= row <= self.row2:
            if self.column1 is None or self.column1 <= column <= self.column2:
                return SingleCellOperand(row, column, ws_name=self.ws_name, source=self.source)
        return BadReference()

    @cached_property
    def is_multidim(self):
        return self.column1 != self.column2 and self.row1 != self.row2


class NamedRangeOperand(CellsOperand):
    def __init__(self, name, *args, **kwargs):
        super(NamedRangeOperand, self).__init__(*args, **kwargs)
        self.name = name

    def address_to_value(self):
        return self.source._named_range_to_cells(self.name, self.ws_name)

    @cached_property
    def value(self):
        if self.ws_name and not self.source._has_worksheet(self.ws_name):
            raise WorksheetNotExist(ws_name=self.ws_name)
        elif not self.source._has_named_range(self.name, self.ws_name):
            raise NamedRangeNotExist(self.name, self.ws_name)
        else:
            return self.address_to_value()

    def get_iter(self):
        return iter(self.value)


@python_2_unicode_compatible
class RPNOperand(OperandLikeObject, OffsetMixin):
    def __init__(self, rpn, *args, **kwargs):
        super(RPNOperand, self).__init__(*args, **kwargs)
        self.rpn = rpn
        self._result = None

    @cached_property
    def evaluated_value(self):
        v = self.rpn.calc(ws_name=self.ws_name, source=self.source)
        if isinstance(v, RPNOperand):
            v = v.evaluated_value
        return v

    def __getattr__(self, item):
        return getattr(self.evaluated_value, item)

    def offset(self, row_offset=0, col_offset=0):
        return RPNOperand(rpn=self.rpn.offset(row_offset, col_offset), ws_name=self.ws_name, source=self.source)

    def __int__(self):
        return int(self.evaluated_value)

    def __float__(self):
        return float(self.evaluated_value)

    def __str__(self):
        return text_type(self.evaluated_value)

    def __trunc__(self):
        return self.__int__()

    def __iter__(self):
        return iter(self.evaluated_value)
