# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from six import python_2_unicode_compatible

from efc.utils import col_str_to_index

__all__ = ('FloatToken', 'IntToken', 'BoolToken',
           'StringToken', 'FunctionToken', 'CellsRangeToken',
           'SingleCellToken', 'NamedRangeToken', 'AddToken',
           'SubtractToken', 'DivideToken', 'MultiplyToken',
           'ConcatToken', 'ExponentToken', 'CompareNotEqToken',
           'CompareGTEToken', 'CompareLTEToken', 'CompareGTToken',
           'CompareLTToken', 'CompareEqToken', 'LeftBracketToken',
           'RightBracketToken', 'SpaceToken', 'Separator',
           'OperandToken', 'OperationToken', 'ArithmeticToken')


@python_2_unicode_compatible
class Token(object):
    pattern = None

    def __init__(self, match):
        self._match = match
        self.src_value = match.group(0)
        self.token_value = self.get_value(match.groupdict())

    @classmethod
    def get_group_pattern(cls):
        return r'(?P<%s>%s)' % (cls.__name__, cls.pattern)

    def get_value(self, m):
        return m[self.__class__.__name__]

    def __str__(self):
        return '<%s, %s>' % (self.__class__.__name__, self.token_value)


class OperandToken(Token):
    pass


class FloatToken(OperandToken):
    pattern = r'(?P<float_value>\d+\.\d+)((?P<float_percent>%)|\b)'

    def get_value(self, m):
        v = float(m['float_value'])
        if m['float_percent'] is not None:
            v /= 100
        return v


class IntToken(OperandToken):
    pattern = r'(?P<int_value>\d+)((?P<int_percent>%)|\b)'

    def get_value(self, m):
        v = int(m['int_value'])
        if m['int_percent'] is not None:
            v /= 100
        return v


class BoolToken(OperandToken):
    pattern = r'\b(TRUE|FALSE)\b'

    def get_value(self, m):
        return super(BoolToken, self).get_value(m) == 'TRUE'


class StringToken(OperandToken):
    pattern = r'"[^"]*"'

    def get_value(self, m):
        return super(StringToken, self).get_value(m)[1:-1]


class AddressToken(OperandToken):
    @staticmethod
    def clean_ws_name(v):
        if v and v.startswith('\''):
            return v[1:-1]
        return v

    def __getattr__(self, item):
        return self.token_value[item]


class SingleCellToken(AddressToken):
    pattern = (r"((?P<q1>')?(\[(?P<s_doc>\w+)\])?(?P<single_ws_name>(?(q1)[^']|\w)+)?(?(q1)'|)!)?"
               r"(?P<column_fixed>\$)?(?P<column>[A-Z]+)(?P<row_fixed>\$)?(?P<row>[0-9]+)\b")

    def get_value(self, m):
        return {
            'ws_name': self.clean_ws_name(m['single_ws_name']),
            'row': int(m['row']),
            'column': col_str_to_index(m['column']),
            'row_fixed': bool(m['row_fixed']),
            'column_fixed': bool(m['column_fixed']),
        }


class CellsRangeToken(AddressToken):
    pattern = (r"((?P<q2>')?(\[(?P<r_doc>\w+)\])?(?P<range_ws_name>(?(q2)[^']|\w)+)?(?(q2)'|)!)?"
               r"((?P<column1_fixed>\$)?(?P<column1>[A-Z]+))?((?P<row1_fixed>\$)?(?P<row1>[0-9]+))?"
               r":((?P<column2_fixed>\$)?(?P<column2>[A-Z]+))?((?P<row2_fixed>\$)?(?P<row2>[0-9]+))?\b")

    def get_value(self, m):
        return {
            'ws_name': self.clean_ws_name(m['range_ws_name']),
            'row1': int(m['row1']) if m['row1'] is not None else None,
            'column1': col_str_to_index(m['column1']) if m['column1'] is not None else None,
            'row2': int(m['row2']) if m['row2'] is not None else None,
            'column2': col_str_to_index(m['column2']) if m['column2'] is not None else None,
            'row1_fixed': bool(m['row1_fixed']),
            'row2_fixed': bool(m['row2_fixed']),
            'column1_fixed': bool(m['column1_fixed']),
            'column2_fixed': bool(m['column2_fixed']),
        }


class NamedRangeToken(AddressToken):
    pattern = (r"((?P<q3>')?(\[(?P<n_doc>\w+)\])?(?P<named_range_ws_name>(?(q3)[^']|\w)+)?(?(q3)'|)!)?"
               r"(?P<range_name>\w+)")

    def get_value(self, m):
        return {
            'ws_name': self.clean_ws_name(m['named_range_ws_name']),
            'name': m['range_name']
        }


class OperationToken(Token):
    pass


class FunctionToken(OperationToken):
    pattern = r'(?P<prefix>_(xlfn|xludf)\.)?(?P<func_name>[A-Z]+)(?=\()'

    def get_value(self, m):
        return m['func_name']


class ArithmeticToken(OperationToken):
    pass


class AddToken(ArithmeticToken):
    pattern = r'\+'


class SubtractToken(ArithmeticToken):
    pattern = r'\-'


class DivideToken(ArithmeticToken):
    pattern = r'/'


class MultiplyToken(ArithmeticToken):
    pattern = r'\*'


class ConcatToken(ArithmeticToken):
    pattern = r'\&'


class ExponentToken(ArithmeticToken):
    pattern = r'\^'


class CompareNotEqToken(ArithmeticToken):
    pattern = r'\<\>'


class CompareGTEToken(ArithmeticToken):
    pattern = r'\>\='


class CompareLTEToken(ArithmeticToken):
    pattern = r'\<\='


class CompareGTToken(ArithmeticToken):
    pattern = r'\>'


class CompareLTToken(ArithmeticToken):
    pattern = r'\<'


class CompareEqToken(ArithmeticToken):
    pattern = r'\='


class LeftBracketToken(Token):
    pattern = r'\('


class RightBracketToken(Token):
    pattern = r'\)'


class SpaceToken(Token):
    pattern = r'[ \n]+'


class Separator(Token):
    pattern = r','
