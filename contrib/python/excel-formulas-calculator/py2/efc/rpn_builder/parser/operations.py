# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from functools import wraps

from efc.rpn_builder.parser.functions import EXCEL_FUNCTIONS
from efc.rpn_builder.parser.operands import FunctionNotSupported, OperandLikeObject, SimpleOperand


def excel_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if not isinstance(result, OperandLikeObject):
            result = SimpleOperand(result)
        return result

    return wrapper


class Operation(object):
    operands_count = None
    priority = None

    def __init__(self, f_name):
        self.f_name = f_name
        self.operands_count = 1

    @property
    def f(self):
        return excel_function(EXCEL_FUNCTIONS[self.f_name])

    @property
    def is_exists(self):
        return self.f_name in EXCEL_FUNCTIONS

    def eval(self, *args):
        if self.is_exists:
            return self.f(*args)
        else:
            return FunctionNotSupported(self.f_name)


class ArithmeticOperation(Operation):
    def __init__(self, f_name, priority):
        super(ArithmeticOperation, self).__init__(f_name)
        self.operands_count = 2
        self.priority = priority


class FunctionOperation(Operation):
    def __init__(self, f_name):
        super(FunctionOperation, self).__init__(f_name)
        self.operands_count = 1
        self.priority = float('inf')
