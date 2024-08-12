import os
import string
import sys
import time
import uuid
import json
import socket
import inspect
import hashlib
import platform
import threading
import traceback
import collections

from traceback import format_exception_only


def md5(*args):
    m = hashlib.md5()
    for arg in args:
        if not isinstance(arg, bytes):
            if not isinstance(arg, str):
                arg = repr(arg)
            arg = arg.encode('utf-8')
        m.update(arg)
    return m.hexdigest()


def uuid4():
    return str(uuid.uuid4())


def now():
    return int(round(1000 * time.time()))


def platform_label():
    major_version, *_ = platform.python_version_tuple()
    implementation = platform.python_implementation().lower()
    return f'{implementation}{major_version}'


def thread_tag():
    return '{0}-{1}'.format(os.getpid(), threading.current_thread().name)


def host_tag():
    return socket.gethostname()


def represent(item):
    """
    >>> represent(None)
    'None'

    >>> represent(123)
    '123'

    >>> represent('hi')
    "'hi'"

    >>> represent('привет')
    "'привет'"

    >>> represent(bytearray([0xd0, 0xbf]))  # doctest: +ELLIPSIS
    "<... 'bytearray'>"

    >>> from struct import pack
    >>> represent(pack('h', 0x89))
    "<class 'bytes'>"

    >>> represent(int)
    "<class 'int'>"

    >>> represent(represent)  # doctest: +ELLIPSIS
    '<function represent at ...>'

    >>> represent([represent])  # doctest: +ELLIPSIS
    '[<function represent at ...>]'

    >>> class ClassWithName:
    ...     pass

    >>> represent(ClassWithName)
    "<class 'utils.ClassWithName'>"
    """

    if isinstance(item, str):
        return f"'{item}'"
    elif isinstance(item, (bytes, bytearray)):
        return repr(type(item))
    else:
        return repr(item)


def func_parameters(func, *args, **kwargs):
    """
    >>> def helper(func):
    ...     def wrapper(*args, **kwargs):
    ...         params = func_parameters(func, *args, **kwargs)
    ...         print(list(params.items()))
    ...         return func(*args, **kwargs)
    ...     return wrapper

    >>> @helper
    ... def args(a, b):
    ...     pass

    >>> args(1, 2)
    [('a', '1'), ('b', '2')]

    >>> args(*(1,2))
    [('a', '1'), ('b', '2')]

    >>> args(1, b=2)
    [('a', '1'), ('b', '2')]

    >>> @helper
    ... def kwargs(a=1, b=2):
    ...     pass

    >>> kwargs()
    [('a', '1'), ('b', '2')]

    >>> kwargs(a=3, b=4)
    [('a', '3'), ('b', '4')]

    >>> kwargs(b=4, a=3)
    [('a', '3'), ('b', '4')]

    >>> kwargs(a=3)
    [('a', '3'), ('b', '2')]

    >>> kwargs(b=4)
    [('a', '1'), ('b', '4')]

    >>> @helper
    ... def args_kwargs(a, b, c=3, d=4):
    ...     pass

    >>> args_kwargs(1, 2)
    [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4')]

    >>> args_kwargs(1, 2, d=5)
    [('a', '1'), ('b', '2'), ('c', '3'), ('d', '5')]

    >>> args_kwargs(1, 2, 5, 6)
    [('a', '1'), ('b', '2'), ('c', '5'), ('d', '6')]

    >>> args_kwargs(1, b=2)
    [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4')]

    >>> @helper
    ... def varargs(*a):
    ...     pass

    >>> varargs()
    []

    >>> varargs(1, 2)
    [('a', '(1, 2)')]

    >>> @helper
    ... def keywords(**a):
    ...     pass

    >>> keywords()
    []

    >>> keywords(a=1, b=2)
    [('a', '1'), ('b', '2')]

    >>> @helper
    ... def args_varargs(a, b, *c):
    ...     pass

    >>> args_varargs(1, 2)
    [('a', '1'), ('b', '2')]

    >>> args_varargs(1, 2, 2)
    [('a', '1'), ('b', '2'), ('c', '(2,)')]

    >>> @helper
    ... def args_kwargs_varargs(a, b, c=3, **d):
    ...     pass

    >>> args_kwargs_varargs(1, 2)
    [('a', '1'), ('b', '2'), ('c', '3')]

    >>> args_kwargs_varargs(1, 2, 4, d=5, e=6)
    [('a', '1'), ('b', '2'), ('c', '4'), ('d', '5'), ('e', '6')]

    >>> @helper
    ... def args_kwargs_varargs_keywords(a, b=2, *c, **d):
    ...     pass

    >>> args_kwargs_varargs_keywords(1)
    [('a', '1'), ('b', '2')]

    >>> args_kwargs_varargs_keywords(1, 2, 4, d=5, e=6)
    [('a', '1'), ('b', '2'), ('c', '(4,)'), ('d', '5'), ('e', '6')]

    >>> class Class:
    ...     @staticmethod
    ...     @helper
    ...     def static_args(a, b):
    ...         pass
    ...
    ...     @classmethod
    ...     @helper
    ...     def method_args(cls, a, b):
    ...         pass
    ...
    ...     @helper
    ...     def args(self, a, b):
    ...         pass

    >>> cls = Class()

    >>> cls.args(1, 2)
    [('a', '1'), ('b', '2')]

    >>> cls.method_args(1, 2)
    [('a', '1'), ('b', '2')]

    >>> cls.static_args(1, 2)
    [('a', '1'), ('b', '2')]

    """
    parameters = {}
    arg_spec = inspect.getfullargspec(func)
    arg_order = list(arg_spec.args)
    args_dict = dict(zip(arg_spec.args, args))

    if arg_spec.defaults:
        kwargs_defaults_dict = dict(zip(arg_spec.args[-len(arg_spec.defaults):], arg_spec.defaults))
        parameters.update(kwargs_defaults_dict)

    if arg_spec.varargs:
        arg_order.append(arg_spec.varargs)
        varargs = args[len(arg_spec.args):]
        parameters.update({arg_spec.varargs: varargs} if varargs else {})

    if arg_spec.args and arg_spec.args[0] in ['cls', 'self']:
        args_dict.pop(arg_spec.args[0], None)

    if kwargs:
        if sys.version_info < (3, 7):
            # Sort alphabetically as old python versions does
            # not preserve call order for kwargs.
            arg_order.extend(sorted(list(kwargs.keys())))
        else:
            # Keep py3.7 behaviour to preserve kwargs order
            arg_order.extend(list(kwargs.keys()))
        parameters.update(kwargs)

    parameters.update(args_dict)

    items = parameters.items()
    sorted_items = sorted(
        map(
            lambda kv: (kv[0], represent(kv[1])),
            items
        ),
        key=lambda x: arg_order.index(x[0])
    )

    return collections.OrderedDict(sorted_items)


def format_traceback(exc_traceback):
    return ''.join(traceback.format_tb(exc_traceback)) if exc_traceback else None


def format_exception(etype, value):
    """
    >>> import sys

    >>> try:
    ...     assert False, 'Привет'
    ... except AssertionError:
    ...     etype, e, _ = sys.exc_info()
    ...     format_exception(etype, e) # doctest: +ELLIPSIS
    'AssertionError: ...\\n'

    >>> try:
    ...     assert False, 'Привет'
    ... except AssertionError:
    ...     etype, e, _ = sys.exc_info()
    ...     format_exception(etype, e) # doctest: +ELLIPSIS
    'AssertionError: ...\\n'

    >>> try:
    ...    compile("bla 'Привет'", "fake.py", "exec")
    ... except SyntaxError:
    ...    etype, e, _ = sys.exc_info()
    ...    format_exception(etype, e) # doctest: +ELLIPSIS
    '  File "fake.py", line 1...SyntaxError: invalid syntax\\n'

    >>> try:
    ...    compile("bla 'Привет'", "fake.py", "exec")
    ... except SyntaxError:
    ...    etype, e, _ = sys.exc_info()
    ...    format_exception(etype, e) # doctest: +ELLIPSIS
    '  File "fake.py", line 1...SyntaxError: invalid syntax\\n'

    >>> from hamcrest import assert_that, equal_to

    >>> try:
    ...     assert_that('left', equal_to('right'))
    ... except AssertionError:
    ...     etype, e, _ = sys.exc_info()
    ...     format_exception(etype, e) # doctest: +ELLIPSIS
    "AssertionError: \\nExpected:...but:..."

    >>> try:
    ...     assert_that('left', equal_to('right'))
    ... except AssertionError:
    ...     etype, e, _ = sys.exc_info()
    ...     format_exception(etype, e) # doctest: +ELLIPSIS
    "AssertionError: \\nExpected:...but:..."
    """
    return '\n'.join(format_exception_only(etype, value)) if etype or value else None


def get_testplan():
    planned_tests = []
    file_path = os.environ.get("ALLURE_TESTPLAN_PATH")

    if file_path and os.path.exists(file_path):
        with open(file_path, 'r') as plan_file:
            plan = json.load(plan_file)
            planned_tests = plan.get("tests", [])

    return planned_tests


class SafeFormatter(string.Formatter):
    """
    Format string safely - skip any non-passed keys
    >>> f = SafeFormatter().format

    Make sure we don't broke default formatting behaviour
    >>> f("literal string")
    'literal string'
    >>> f("{expected.format}", expected=str)
    "<method 'format' of 'str' objects>"
    >>> f("{expected[0]}", expected=["value"])
    'value'
    >>> f("{expected[0]}", expected=123)
    Traceback (most recent call last):
    ...
    TypeError: 'int' object is not subscriptable
    >>> f("{expected[0]}", expected=[])
    Traceback (most recent call last):
    ...
    IndexError: list index out of range
    >>> f("{expected.format}", expected=int)
    Traceback (most recent call last):
    ...
    AttributeError: type object 'int' has no attribute 'format'

    Check that unexpected keys do not cause some errors
    >>> f("{expected} {unexpected}", expected="value")
    'value {unexpected}'
    >>> f("{unexpected[0]}", expected=["value"])
    '{unexpected[0]}'
    >>> f("{unexpected.format}", expected=str)
    '{unexpected.format}'
    """

    class SafeKeyOrIndexError(Exception):
        pass

    def get_field(self, field_name, args, kwargs):
        try:
            return super().get_field(field_name, args, kwargs)
        except self.SafeKeyOrIndexError:
            return "{" + field_name + "}", field_name

    def get_value(self, key, args, kwargs):
        try:
            return super().get_value(key, args, kwargs)
        except (KeyError, IndexError):
            raise self.SafeKeyOrIndexError()
