# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import re
from abc import ABCMeta
from calendar import isleap, monthrange
from collections import defaultdict
from functools import wraps
from itertools import chain, groupby

from six import add_metaclass, integer_types, iteritems, string_types, text_type
from six.moves import range, zip_longest

from efc.rpn_builder.parser.operands import (
    BadReference,
    CellRangeOperand,
    CellSetOperand,
    EmptyOperand,
    ErrorOperand,
    HyperlinkOperand,
    NamedRangeOperand,
    NotFoundErrorOperand,
    NumErrorOperand,
    RPNOperand,
    SetOperand,
    SimpleOperand,
    SimpleSetOperand,
    SingleCellOperand,
    ValueErrorOperand,
    ValueNotAvailable,
)
from efc.utils import is_float, parse_date

__all__ = ('EXCEL_FUNCTIONS',)


@add_metaclass(ABCMeta)
class CompareAbstract(object):
    def __init__(self, symbol, func):
        self._symbol = symbol
        self._func = func

    @staticmethod
    def get_type_id(obj):
        if isinstance(obj, bool):
            return 2
        elif isinstance(obj, string_types):
            return 1
        return 0


class SimpleCompare(CompareAbstract):
    """1 > 3, A1 < B2"""

    def __hash__(self):
        return hash((self._symbol, self._func))

    def __eq__(self, other):
        return isinstance(other, SimpleCompare) and hash(other) == hash(self)

    @classmethod
    def get_op1_type(cls, op1, op2):
        if op1.value is None:
            if isinstance(op1, SingleCellOperand) and op1.linked_cell != op1 or not isinstance(op2.value, string_types):
                op1_type = (0, 0)
            elif not isinstance(op2.value, string_types):
                op1_type = (0, 0)
            else:
                op1_type = (1, '')
        else:
            op1_type = (cls.get_type_id(op1.value), op1.value)
        return op1_type

    @classmethod
    def get_op_types(cls, op1, op2):
        return cls.get_op1_type(op1, op2), cls.get_op1_type(op2, op1)

    def __call__(self, op1, op2):
        return self._func(*self.get_op_types(op1, op2))


class FunctionsCompare(CompareAbstract):
    """COUNTIF(A;B, \">=10\")"""

    def __init__(self, check_2_types, *args, **kwargs):
        self._check_2_types = check_2_types
        super(FunctionsCompare, self).__init__(*args, **kwargs)

    def __hash__(self):
        return hash((self._check_2_types, self._symbol, self._func))

    def __eq__(self, other):
        return isinstance(other, FunctionsCompare) and hash(other) == hash(self)

    def _get_op_type(self, op):
        if isinstance(op, SingleCellOperand):
            if op.linked_cell == op:
                if op.value is None:
                    op1_type = (1, '')
                else:
                    op1_type = (self.get_type_id(op.value), op.value)
            else:
                if op.value is None:
                    op1_type = (0, 0)
                else:
                    op1_type = (self.get_type_id(op.value), op.value)
        elif op.value is None:
            op1_type = (1, '')
        else:
            op1_type = (self.get_type_id(op.value), op.value)
        return op1_type

    @staticmethod
    def _same_linked_cell(op1, op2):
        if not isinstance(op1, SingleCellOperand):
            return False
        if not isinstance(op2, SingleCellOperand):
            return False

        return all(getattr(op1.linked_cell, key) == getattr(op2.linked_cell, key)
                   for key in ('row', 'column', 'source', 'ws_name'))

    def __call__(self, op1, op2):
        op1_type = self._get_op_type(op1)
        op2_type = self._get_op_type(op2)

        if self._same_linked_cell(op1, op2):
            res = self._func(op1_type, op1_type)
        else:
            check = [op2_type]
            if self._check_2_types:
                if op2_type[0] == 0:
                    tmp = SimpleOperand(str(op2.value))
                elif op2_type[0] == 1:
                    tmp = SimpleOperand(float(op2.value))
                else:
                    tmp = SimpleOperand({True: 'TRUE', False: 'FALSE'}[op2.value])
                check.append(self._get_op_type(tmp))

            res = False
            for check_op_type in check:
                if self._symbol in ('<>', '='):
                    res |= self._func(op1_type, check_op_type)
                elif op1.value is op2.value is None:
                    res |= True
                elif op1.value is None and check_op_type[0] == 0 and op1.linked_cell == op1:
                    res |= False
                elif op1.value is None and check_op_type[0] == 1 and op2.value != '':
                    res |= False
                elif op1_type[0] != check_op_type[0]:
                    res |= False
                else:
                    res |= self._func(op1_type, check_op_type)
        return res


def set_mixin(foo):
    @wraps(foo)
    def wrapper(op1, op2):
        fill_value = None

        op1_is_set = isinstance(op1, SetOperand)
        op2_is_set = isinstance(op2, SetOperand)

        if op1_is_set or op2_is_set:
            if not op1_is_set:
                op1, fill_value = tuple(), op1
            elif not op2_is_set:
                op2, fill_value = tuple(), op2

            result = SimpleSetOperand()
            for v1, v2 in zip_longest(op1, op2, fillvalue=fill_value):
                result.add_cell(ValueNotAvailable() if v1 is None or v2 is None else SimpleOperand(foo(v1, v2)))
        else:
            result = foo(op1, op2)
        return result

    return wrapper


def operand_to_final_operand(op):
    while True:
        if isinstance(op, NamedRangeOperand):
            op = op.value
        elif isinstance(op, RPNOperand):
            op = op.evaluated_value
        else:
            return op


@set_mixin
def add(op1, op2):
    return op1.digit + op2.digit


def add_func(op1, op2=None):
    if isinstance(op2, EmptyOperand):
        op2 = None

    if op2 is not None:
        return add(op1, op2)
    else:
        return op1.digit


@set_mixin
def sub(op1, op2):
    return op1.digit - op2.digit


def subtract_func(op1, op2=None):
    if isinstance(op2, EmptyOperand):
        op2 = None

    if op2 is not None:
        return sub(op1, op2)
    else:
        return -op1.digit


@set_mixin
def divide_func(op1, op2):
    return op1.digit / op2.digit


@set_mixin
def multiply_func(op1, op2):
    return op1.digit * op2.digit


@set_mixin
def concat_func(op1, op2):
    if isinstance(op1, ErrorOperand):
        raise op1
    if isinstance(op2, ErrorOperand):
        raise op2
    return op1.string + op2.string


@set_mixin
def exponent_func(op1, op2):
    return op1.digit**op2.digit


@set_mixin
def compare_not_eq_func(op1, op2):
    return op1 != op2


@set_mixin
def compare_gte_func(op1, op2):
    return op1 >= op2


@set_mixin
def compare_lte_func(op1, op2):
    return op1 <= op2


@set_mixin
def compare_gt_func(op1, op2):
    return op1 > op2


@set_mixin
def compare_lt_func(op1, op2):
    return op1 < op2


@set_mixin
def compare_eq_func(op1, op2):
    return op1 == op2


def iter_elements(*args):
    for arg in args:
        if isinstance(arg, RPNOperand):
            arg = arg.evaluated_value

        if isinstance(arg, (CellRangeOperand, CellSetOperand)):
            for cell in arg:
                yield cell
        else:
            yield arg


def iter_digits(*args):
    for op in iter_elements(*args):
        if not op.is_blank:
            try:
                yield op.digit
            except (ValueError, TypeError):
                pass


def sum_func(*args):
    return sum(d or 0 for d in iter_digits(*args))


def mod_func(op1, op2):
    return op1.digit % op2.digit


def if_func(expr_op, op1, op2=None):
    if op2 is None:
        op2 = False
    return op1 if expr_op.value else op2


def ifs_func(*args):
    for i in range(0, len(args), 2):
        if args[i].value:
            return args[i + 1].value
    else:
        raise ValueNotAvailable


def if_error_func(op1, op2):
    if isinstance(op1, RPNOperand):
        op1 = op1.evaluated_value
    return op2 if isinstance(op1, ErrorOperand) else op1


def rpn_operand_value(foo):
    @wraps(foo)
    def wrapper(op):
        if isinstance(op, RPNOperand):
            op = op.evaluated_value
        return foo(op)

    return wrapper


@rpn_operand_value
def is_error_func(op):
    return isinstance(op, ErrorOperand)


def max_func(*args):
    return max(list(d or 0 for d in iter_digits(*args)) or [0])


def min_func(*args):
    return min(list(d or 0 for d in iter_digits(*args)) or [0])


def left_func(op1, op2=1):
    if isinstance(op2, EmptyOperand):
        op2 = 1

    return op1.string[:int(op2)]


def right_func(op1, op2=1):
    if isinstance(op2, EmptyOperand):
        op2 = 1

    return op1.string[-int(op2):]


def mid_func(op1, op2, op3):
    left = int(op2) - 1
    right = left + int(op3)
    return op1.string[left:right]


def is_blank_func(a):
    while True:
        if isinstance(a, (NamedRangeOperand, SingleCellOperand)):
            a = SimpleOperand(a.address_to_value())
        elif isinstance(a, RPNOperand):
            a = a.evaluated_value
        else:
            break

    try:
        it = iter(a)
    except TypeError:
        return a.value is None

    new_set = SimpleSetOperand()
    for v in it:
        new_set.add_cell(SimpleOperand(v.value is None))
    return new_set


def or_function(*args):
    for op in iter_elements(*args):
        v = op.value
        if v is not None and not isinstance(v, string_types) and v:
            return True
    return False


def and_function(*args):
    for op in iter_elements(*args):
        v = op.value
        if v is not None and not isinstance(v, string_types) and not v:
            return False
    return True


def not_func(op):
    return not op.value


def small_function(r, op):
    items = sorted(iter_digits(r))
    index = int(op) - 1
    try:
        return items[index]
    except IndexError:
        return ValueErrorOperand()


def large_function(r, op):
    items = sorted(iter_digits(r), reverse=True)
    index = int(op) - 1
    try:
        return items[index]
    except IndexError:
        return ValueErrorOperand()


def round_function(a, b):
    b = int(b)
    v = round(a.digit, b)
    if b == 0:
        v = int(v)
    return v


def round_down_function(a, b):
    b = int(b)
    base = 10**b
    v = a.digit * base // 1 / base
    if b == 0:
        v = int(v)
    return v


def floor_function(a, multiple):
    multiple = int(multiple)
    return int(a.digit / multiple) * multiple


def count_function(*args):
    return len([op for op in iter_elements(*args) if isinstance(op.value, (integer_types, float))])


def abs_function(a):
    return abs(a.digit)


def match_function(op1, r, match_type=1):
    if isinstance(match_type, EmptyOperand):
        match_type = None

    match_type = 0 if match_type is None else int(match_type)

    if match_type == 1:
        match_idx = None
        if isinstance(r, CellRangeOperand) and r.is_multidim:
            raise NotFoundErrorOperand

        for idx, item in enumerate(r, 1):
            if EXCEL_FUNCTIONS['>'](item, op1):
                if match_idx is not None:
                    break
                else:
                    raise NotFoundErrorOperand
            elif EXCEL_FUNCTIONS['='](item, op1):
                match_idx = idx
        if match_idx is None:
            raise NotFoundErrorOperand
        idx = match_idx
    elif match_type == -1:
        for idx, item in enumerate(r, 1):
            if EXCEL_FUNCTIONS['>'](item, op1):
                break
        else:
            idx = None
    else:
        for idx, item in enumerate(r, 1):
            if EXCEL_FUNCTIONS['='](item, op1):
                break
        else:
            idx = None
    if idx is None:
        raise NotFoundErrorOperand()
    return idx


COUNT_IF_EXPR = re.compile(r'^(?P<symbol><=|>=|<>|>|<|=)(?P<value>.+)$')


def get_check_function(expr):
    if isinstance(expr.value, string_types):
        match = COUNT_IF_EXPR.search(expr.value)
        if match:
            match = match.groupdict()
            operation = match['symbol']
            value_is_float = is_float(match['value'])
            if value_is_float and operation != '=':
                value = float(match['value'])
            else:
                value = match['value']
            operand = SimpleOperand(value)
        else:
            operation = '='
            operand = expr
            value_is_float = is_float(expr.value)
    else:
        operation = '='
        operand = expr
        value_is_float = is_float(expr.value)

    check_2_types = value_is_float and operation == '='
    check = FunctionsCompare(check_2_types, operation, COMPARE_FUNCTIONS[operation])
    return check, operand


def countif_function(cells, expr):
    check, operand = get_check_function(expr)
    return len([op for op in cells.value if check(op, operand)])


def counta_function(cells):
    return len([op for op in cells.value if op.value is not None])


def get_checks_from_args(args):
    args = iter(args)
    checks = []
    while True:
        check = []
        try:
            check.append(next(args))
        except StopIteration:
            break

        check.extend(get_check_function(next(args)))
        checks.append(check)
    return checks


def ifs_indexes(*args):
    good_indexes = None

    checks = get_checks_from_args(args)

    for op_range, check, expr in checks:
        check_good_indexes = None
        key = None
        if op_range.source and isinstance(op_range, CellRangeOperand):
            cache = op_range.source._caches['ifs'] if op_range.source._caches is not None else None

            if cache is not None:
                key = (op_range.ws_name, op_range.row1, op_range.column1, op_range.row2,
                       op_range.column2, check, expr.value)
                check_good_indexes = cache.get(key)
        else:
            cache = None

        if check_good_indexes is None:
            check_good_indexes = set()
            for idx, item in enumerate(op_range, 1):
                if item is None:
                    raise ValueErrorOperand
                else:
                    # convert expr value to item type
                    if isinstance(item.value, string_types) and not isinstance(expr.value, string_types):
                        expr = SimpleOperand(expr.string)
                    elif isinstance(item.value, (integer_types, float)) and not isinstance(expr.value,
                                                                                           (integer_types, float)):
                        try:
                            expr = SimpleOperand(expr.digit)
                        except ValueError:
                            pass

                    if check(item, expr):
                        check_good_indexes.add(idx)

            if cache is not None:
                cache[key] = check_good_indexes

        if good_indexes is None:
            good_indexes = check_good_indexes.copy()
        else:
            good_indexes &= check_good_indexes

        if not good_indexes:
            break

    return good_indexes


def sum_ifs_function(op1, *args):
    good_indexes = ifs_indexes(*args)
    return sum_func(*(c for idx, c in enumerate(op1, 1) if idx in good_indexes))


def sumproduct_function(op1, *args):
    operands = []
    r_sizes = s_sizes = None
    for item in chain([op1], args):
        item = operand_to_final_operand(item)

        if isinstance(item, SetOperand):
            item_sizes = {(item.rows_count, item.columns_count), (item.columns_count, item.rows_count)}
            if s_sizes is None:
                s_sizes = item_sizes
            elif s_sizes != item_sizes or r_sizes is not None and not r_sizes.intersection(item_sizes):
                raise NotFoundErrorOperand
        elif isinstance(item, CellRangeOperand):
            item_sizes = {(item.row2 - item.row1 + 1, item.column2 - item.column1 + 1)}
            if r_sizes is None:
                r_sizes = item_sizes
            elif r_sizes != item_sizes or s_sizes is not None and not s_sizes.intersection(item_sizes):
                raise NotFoundErrorOperand
        else:
            raise NotFoundErrorOperand

        operands.append(item)

    result = 0
    for ops in zip(*operands):
        partial_result = ops[0].digit
        for op in ops[1:]:
            partial_result = partial_result * op.digit
        result += partial_result
    return result


def sum_if_function(r, expr, op1=None):
    return sum_ifs_function(op1 or r, r, expr)


def concatenate(*args):
    return ''.join(i.string for i in iter_elements(*args))


def average_function(*args):
    values = list(iter_digits(*args))
    return sum(values) / len(values)


def average_ifs_function(op1, *args):
    good_indexes = ifs_indexes(*args)
    return average_function(*(c for idx, c in enumerate(op1, 1) if idx in good_indexes))


def count_blank_function(cells):
    return len([op for op in iter_elements(cells) if op.is_blank])


def count_ifs_function(*args):
    return len(ifs_indexes(*args))


def offset_function(cell, row_offset, col_offset, height=None, width=None):
    height_is_none = height is None or isinstance(height, EmptyOperand)
    width_is_none = width is None or isinstance(width, EmptyOperand)

    if isinstance(cell, SingleCellOperand):
        height = int(height) if not height_is_none else 1
        width = int(width) if not width_is_none else 1

        column = cell.column
        row = cell.row
    elif isinstance(cell, CellRangeOperand):
        height = int(height) if not height_is_none else cell.row2 - cell.row1 + 1
        width = int(width) if not width_is_none else cell.column2 - cell.column1 + 1

        column = cell.column1
        row = cell.row1
    else:
        return ValueErrorOperand()

    if height == width == 1:
        return SingleCellOperand(row=row + int(row_offset), column=column + int(col_offset),
                                 ws_name=cell.ws_name, source=cell.source)
    else:
        return CellRangeOperand(row1=row + int(row_offset),
                                column1=column + int(col_offset),
                                row2=row + int(row_offset) + height - 1,
                                column2=column + int(col_offset) + width - 1,
                                ws_name=cell.ws_name, source=cell.source)


def vlookup_function(op, rg, column, flag=None):
    rg = operand_to_final_operand(rg)

    if isinstance(flag, EmptyOperand):
        flag = None

    first_col = rg.offset()
    first_col.column2 = first_col.column1

    op = operand_to_final_operand(op)
    if isinstance(op, (SetOperand, CellRangeOperand)):
        check = list(op)
    else:
        check = [op]

    result = CellSetOperand()
    for op in check:
        if flag is not None and flag.digit or flag is None:
            idx = match_function(op, first_col, 1)
        else:
            idx = match_function(op, first_col, 0)
        result.add_cell(SingleCellOperand(row=(rg.row1 or 1) + idx - 1, column=(rg.column1 or 1) + column.digit - 1,
                                          ws_name=rg.ws_name, source=rg.source))

    if result.columns_count == 1:
        return next(iter(result))
    else:
        return result


def hlookup_function(op, rg, row, flag=None):
    if isinstance(flag, EmptyOperand):
        flag = None

    first_row = rg.offset()
    first_row.row2 = first_row.row1

    if flag is not None and flag.digit or flag is None:
        idx = match_function(op, first_row, 1)
    else:
        idx = match_function(op, first_row, 0)
    return SingleCellOperand(row=(rg.row1 or 1) + row.digit - 1, column=(rg.column1 or 1) + idx - 1,
                             ws_name=rg.ws_name, source=rg.source)


def hyperlink_function(link, text=None):
    if isinstance(link, RPNOperand):
        link = link.evaluated_value

    if isinstance(text, RPNOperand):
        text = text.evaluated_value

    return HyperlinkOperand(link.value, text.value if text is not None else None)


def index_function(rg, row, column=None):
    if isinstance(column, EmptyOperand):
        column = None

    if isinstance(rg, RPNOperand):
        rg = rg.evaluated_value

    if isinstance(rg, SetOperand):
        set_type = SimpleSetOperand
        row1 = 1
        row2 = rg.rows_count
        column1 = 1
        column2 = rg.columns_count
    else:
        set_type = CellSetOperand
        row1 = rg.row1
        row2 = rg.row2
        column1 = rg.column1
        column2 = rg.column2

    rg_size = 2 if row1 != row2 and column1 != column2 else 1

    row = row.digit
    if column is not None:
        column = column.digit
    elif rg_size == 1:
        column = 1

    if rg_size == 1:
        if column1 is not None and column != 1:
            return BadReference()
    else:
        if column is None or row == 0 or column == 0:
            return BadReference()

    if row == 0:
        result = set_type()
        for c in range(column1, column2 + 1):
            result.add_cell(rg.get_cell(1, c))
    elif column == 0:
        result = set_type()
        for r in range(row1, row2 + 1):
            result.add_cell(rg.get_cell(r, 1))
    else:
        result = rg.get_cell(row, column)

    return result


def substitute_func(text, old_text, new_text, instance_num=None):
    if isinstance(instance_num, EmptyOperand):
        instance_num = None

    instance_num = instance_num.digit if instance_num is not None else -1
    return text.string.replace(old_text.string, new_text.string, instance_num)


def search_func(pattern, source, start_position=None):
    if isinstance(start_position, EmptyOperand):
        start_position = None

    try:
        if start_position is not None:
            start_position = start_position.digit - 1
        else:
            start_position = 0

        return source.string[start_position:].lower().index(pattern.string.lower()) + 1
    except ValueError:
        return ValueErrorOperand()


TRIM_REGEXP = re.compile(r' {2,}')


def trim_func(op):
    value = op.string.strip()
    value = TRIM_REGEXP.sub(' ', value)
    return value


def unique_func(op1, by_col=None, exactly_once=None):
    if isinstance(by_col, EmptyOperand) or by_col is None:
        by_col = False
    else:
        by_col = by_col.value

    if isinstance(exactly_once, EmptyOperand) or exactly_once is None:
        exactly_once = False
    else:
        exactly_once = exactly_once.value

    if isinstance(op1, SingleCellOperand):
        cells_set = CellSetOperand()
        cells_set.add_cell(op1)
    elif isinstance(op1, CellRangeOperand):
        items = defaultdict(list)
        counter = defaultdict(int)

        get_iterator = lambda: op1.get_columns_iter() if by_col else op1.get_rows_iter()

        for idx, cells in groupby(get_iterator(), lambda x: x[0]):
            key = tuple(c.value for _, c in cells)
            counter[key] += 1
            items[key].append(idx)

        good_items = set()
        for key, ids in iteritems(items):
            count = counter[key]
            if exactly_once and count == 1 or not exactly_once:
                good_items.add(ids[0])

        cells_set = CellSetOperand()
        for idx, cells in groupby(get_iterator(), lambda x: x[0]):
            if idx in good_items:
                if by_col:
                    for row_idx, (_, cell) in enumerate(cells):
                        cells_set.add_cell(cell, row_idx)
                else:
                    cells_set.add_row([c for _, c in cells])
    else:
        # CellSetOperand not supported
        return ValueErrorOperand()

    return cells_set


def len_func(op):
    value = op.string
    return len(value)


def _upper_lower(func, op):
    if isinstance(op, ErrorOperand):
        return op
    return func(op.string)


@rpn_operand_value
def lower_func(op):
    return _upper_lower(text_type.lower, op)


@rpn_operand_value
def upper_func(op):
    return _upper_lower(text_type.upper, op)


def year_frac(dt1, dt2, tp=None):
    if isinstance(tp, EmptyOperand) or tp is None:
        tp = 0
    else:
        tp = tp.digit

    if tp in (0, 4):
        dt1p = parse_date(dt1.string)
        dt2p = parse_date(dt2.string)

        dt1p_day = dt1p.day
        dt2p_day = dt2p.day

        # https://en.wikipedia.org/wiki/Day_count_convention#30/360_US
        if tp == 0:
            lfd1 = monthrange(dt1p.year, dt1p.month)[1]
            lfd2 = monthrange(dt2p.year, dt1p.month)[1]
            if dt1p_day == lfd1 and dt2p_day == lfd2:
                dt2p_day = 30

            if dt1p_day == lfd1:
                dt1p_day = 30

            if dt2p_day == 31 and dt1p_day >= 30:
                dt2p_day = 30

            if dt1p_day == 31:
                dt1p_day = 30

        # https://en.wikipedia.org/wiki/Day_count_convention#30E/360
        else:
            if dt1p_day == 31:
                dt1p_day = 30

            if dt2p_day == 31:
                dt2p_day = 30

        # https://en.wikipedia.org/wiki/Day_count_convention#30/360_methods
        v = (360 * (dt2p.year - dt1p.year) + 30 * (dt2p.month - dt1p.month) + (dt2p_day - dt1p_day)) / 360
    elif tp in (1, 2, 3):
        if tp == 1:
            year_days = 366 if isleap(parse_date(dt2.string).year) else 365
        elif tp == 2:
            year_days = 360
        else:
            year_days = 365
        v = (int(dt2.digit) - int(dt1.digit)) / year_days
    else:
        v = NumErrorOperand()

    return v


def row_func(op):
    if isinstance(op, SingleCellOperand):
        return op.row
    elif isinstance(op, CellRangeOperand):
        return op.row1
    else:
        raise ValueErrorOperand


def column_func(op):
    if isinstance(op, SingleCellOperand):
        return op.column
    elif isinstance(op, CellRangeOperand):
        return op.column1
    else:
        raise ValueErrorOperand


COMPARE_FUNCTIONS = {
    '<>': compare_not_eq_func,
    '>=': compare_gte_func,
    '<=': compare_lte_func,
    '>': compare_gt_func,
    '<': compare_lt_func,
    '=': compare_eq_func,
}

ARITHMETIC_FUNCTIONS = {
    '+': add_func,
    '-': subtract_func,
    '/': divide_func,
    '*': multiply_func,
    '&': concat_func,
    '^': exponent_func,
}

EXCEL_FUNCTIONS = {}
EXCEL_FUNCTIONS.update(ARITHMETIC_FUNCTIONS)
EXCEL_FUNCTIONS.update({k: SimpleCompare(k, v) for k, v in iteritems(COMPARE_FUNCTIONS)})

EXCEL_FUNCTIONS['ABS'] = abs_function
EXCEL_FUNCTIONS['AND'] = and_function
EXCEL_FUNCTIONS['AVERAGE'] = average_function
EXCEL_FUNCTIONS['AVERAGEIFS'] = average_ifs_function

EXCEL_FUNCTIONS['COLUMN'] = column_func
EXCEL_FUNCTIONS['CONCATENATE'] = concatenate
EXCEL_FUNCTIONS['COUNT'] = count_function
EXCEL_FUNCTIONS['COUNTA'] = counta_function
EXCEL_FUNCTIONS['COUNTIF'] = countif_function
EXCEL_FUNCTIONS['COUNTIFS'] = count_ifs_function
EXCEL_FUNCTIONS['COUNTBLANK'] = count_blank_function

EXCEL_FUNCTIONS['FLOOR'] = floor_function

EXCEL_FUNCTIONS['IF'] = if_func
EXCEL_FUNCTIONS['IFS'] = ifs_func
EXCEL_FUNCTIONS['IFERROR'] = if_error_func
EXCEL_FUNCTIONS['INDEX'] = index_function
EXCEL_FUNCTIONS['ISBLANK'] = is_blank_func
EXCEL_FUNCTIONS['ISERROR'] = is_error_func

EXCEL_FUNCTIONS['HLOOKUP'] = hlookup_function
EXCEL_FUNCTIONS['HYPERLINK'] = hyperlink_function

EXCEL_FUNCTIONS['LARGE'] = large_function
EXCEL_FUNCTIONS['LEN'] = len_func
EXCEL_FUNCTIONS['LEFT'] = left_func
EXCEL_FUNCTIONS['LOWER'] = lower_func

EXCEL_FUNCTIONS['MATCH'] = match_function
EXCEL_FUNCTIONS['MAX'] = max_func
EXCEL_FUNCTIONS['MID'] = mid_func
EXCEL_FUNCTIONS['MIN'] = min_func
EXCEL_FUNCTIONS['MOD'] = mod_func

EXCEL_FUNCTIONS['NOT'] = not_func

EXCEL_FUNCTIONS['OFFSET'] = offset_function
EXCEL_FUNCTIONS['OR'] = or_function

EXCEL_FUNCTIONS['RIGHT'] = right_func
EXCEL_FUNCTIONS['ROUND'] = round_function
EXCEL_FUNCTIONS['ROUNDDOWN'] = round_down_function
EXCEL_FUNCTIONS['ROW'] = row_func

EXCEL_FUNCTIONS['SEARCH'] = search_func
EXCEL_FUNCTIONS['SMALL'] = small_function
EXCEL_FUNCTIONS['SUBSTITUTE'] = substitute_func
EXCEL_FUNCTIONS['SUM'] = sum_func
EXCEL_FUNCTIONS['SUMIF'] = sum_if_function
EXCEL_FUNCTIONS['SUMIFS'] = sum_ifs_function
EXCEL_FUNCTIONS['SUMPRODUCT'] = sumproduct_function

EXCEL_FUNCTIONS['TRIM'] = trim_func

EXCEL_FUNCTIONS['UNIQUE'] = unique_func

EXCEL_FUNCTIONS['VLOOKUP'] = vlookup_function

EXCEL_FUNCTIONS['YEARFRAC'] = year_frac
EXCEL_FUNCTIONS['UPPER'] = upper_func
