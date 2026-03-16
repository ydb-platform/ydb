import gc
import os
import pickle
import platform
import sys
import types
import weakref
from datetime import date
from datetime import datetime
from decimal import Decimal
from functools import partial

import pytest

PYPY = '__pypy__' in sys.builtin_module_names

graalpyxfail = pytest.mark.xfail('sys.implementation.name == "graalpy"')

OBJECTS_CODE = """
class TargetBaseClass(object):
    "documentation"

class Target(TargetBaseClass):
    "documentation"

def target():
    "documentation"
    pass
"""

objects = types.ModuleType('objects')
exec(OBJECTS_CODE, objects.__dict__, objects.__dict__)


def test_round(lop):
    proxy = lop.Proxy(lambda: 1.2)
    assert round(proxy) == 1


def test_round_ndigits(lop):
    proxy = lop.Proxy(lambda: 1.49494)
    assert round(proxy, 3) == 1.495


def test_attributes(lop):
    def function1(*args, **kwargs):
        return args, kwargs

    function2 = lop.Proxy(lambda: function1)

    assert function2.__wrapped__ == function1


def test_get_wrapped(lop):
    def function1(*args, **kwargs):
        return args, kwargs

    function2 = lop.Proxy(lambda: function1)

    assert function2.__wrapped__ == function1

    function3 = lop.Proxy(lambda: function2)

    assert function3.__wrapped__ == function1


def test_set_wrapped(lop):
    def function1(*args, **kwargs):
        return args, kwargs

    function2 = lop.Proxy(lambda: function1)

    assert function2 == function1
    assert function2.__wrapped__ is function1
    assert function2.__name__ == function1.__name__

    assert function2.__qualname__ == function1.__qualname__

    function2.__wrapped__ = None

    assert not hasattr(function1, '__wrapped__')

    assert function2 == None  # noqa
    assert function2.__wrapped__ is None
    assert not hasattr(function2, '__name__')

    assert not hasattr(function2, '__qualname__')

    def function3(*args, **kwargs):
        return args, kwargs

    function2.__wrapped__ = function3

    assert function2 == function3
    assert function2.__wrapped__ == function3
    assert function2.__name__ == function3.__name__

    assert function2.__qualname__ == function3.__qualname__


def test_wrapped_attribute(lop):
    def function1(*args, **kwargs):
        return args, kwargs

    function2 = lop.Proxy(lambda: function1)

    function2.variable = True

    assert hasattr(function1, 'variable')
    assert hasattr(function2, 'variable')

    assert function2.variable is True

    del function2.variable

    assert not hasattr(function1, 'variable')
    assert not hasattr(function2, 'variable')

    assert getattr(function2, 'variable', None) is None


def test_class_object_name(lop):
    # Test preservation of class __name__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__name__ == target.__name__


def test_class_object_qualname(lop):
    # Test preservation of class __qualname__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    try:
        __qualname__ = target.__qualname__
    except AttributeError:
        pass
    else:
        assert wrapper.__qualname__ == __qualname__


@pytest.mark.xfail_subclass
def test_class_module_name(lop):
    # Test preservation of class __module__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__module__ == target.__module__


@pytest.mark.xfail_subclass
def test_class_doc_string(lop):
    # Test preservation of class __doc__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__doc__ == target.__doc__


@pytest.mark.xfail_subclass
def test_instance_module_name(lop):
    # Test preservation of instance __module__ attribute.

    target = objects.Target()
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__module__ == target.__module__


@pytest.mark.xfail_subclass
def test_instance_doc_string(lop):
    # Test preservation of instance __doc__ attribute.

    target = objects.Target()
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__doc__ == target.__doc__


def test_function_object_name(lop):
    # Test preservation of function __name__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__name__ == target.__name__


def test_function_object_qualname(lop):
    # Test preservation of function __qualname__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    try:
        __qualname__ = target.__qualname__
    except AttributeError:
        pass
    else:
        assert wrapper.__qualname__ == __qualname__


@pytest.mark.xfail_subclass
def test_function_module_name(lop):
    # Test preservation of function __module__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__module__ == target.__module__


@pytest.mark.xfail_subclass
def test_function_doc_string(lop):
    # Test preservation of function __doc__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__doc__ == target.__doc__


@graalpyxfail
def test_class_of_class(lop):
    # Test preservation of class __class__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__class__ is target.__class__

    assert isinstance(wrapper, type(target))


def test_revert_class_proxying(lop):
    class ProxyWithOldStyleIsInstance(lop.Proxy):
        __class__ = object.__dict__['__class__']

    target = objects.Target()
    wrapper = ProxyWithOldStyleIsInstance(lambda: target)

    assert wrapper.__class__ is ProxyWithOldStyleIsInstance

    assert isinstance(wrapper, ProxyWithOldStyleIsInstance)
    assert not isinstance(wrapper, objects.Target)
    assert not isinstance(wrapper, objects.TargetBaseClass)

    class ProxyWithOldStyleIsInstance2(ProxyWithOldStyleIsInstance):
        pass

    wrapper = ProxyWithOldStyleIsInstance2(lambda: target)

    assert wrapper.__class__ is ProxyWithOldStyleIsInstance2

    assert isinstance(wrapper, ProxyWithOldStyleIsInstance2)
    assert not isinstance(wrapper, objects.Target)
    assert not isinstance(wrapper, objects.TargetBaseClass)


def test_class_of_instance(lop):
    # Test preservation of instance __class__ attribute.

    target = objects.Target()
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__class__ is target.__class__

    assert isinstance(wrapper, objects.Target)
    assert isinstance(wrapper, objects.TargetBaseClass)


@graalpyxfail
def test_class_of_function(lop):
    # Test preservation of function __class__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    assert wrapper.__class__ is target.__class__

    assert isinstance(wrapper, type(target))


def test_dir_of_class(lop):
    # Test preservation of class __dir__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    assert dir(wrapper) == dir(target)


@pytest.mark.xfail_simple
def test_vars_of_class(lop):
    # Test preservation of class __dir__ attribute.

    target = objects.Target
    wrapper = lop.Proxy(lambda: target)

    assert vars(wrapper) == vars(target)


def test_dir_of_instance(lop):
    # Test preservation of instance __dir__ attribute.

    target = objects.Target()
    wrapper = lop.Proxy(lambda: target)

    assert dir(wrapper) == dir(target)


@pytest.mark.xfail_simple
def test_vars_of_instance(lop):
    # Test preservation of instance __dir__ attribute.

    target = objects.Target()
    wrapper = lop.Proxy(lambda: target)

    assert vars(wrapper) == vars(target)


def test_dir_of_function(lop):
    # Test preservation of function __dir__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    assert dir(wrapper) == dir(target)


@pytest.mark.xfail_simple
def test_vars_of_function(lop):
    # Test preservation of function __dir__ attribute.

    target = objects.target
    wrapper = lop.Proxy(lambda: target)

    assert vars(wrapper) == vars(target)


def test_function_no_args(lop):
    _args = ()
    _kwargs = {}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = lop.Proxy(lambda: function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_function_args(lop):
    _args = (1, 2)
    _kwargs = {}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = lop.Proxy(lambda: function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_function_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = lop.Proxy(lambda: function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_function_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    def function(*args, **kwargs):
        return args, kwargs

    wrapper = lop.Proxy(lambda: function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_no_args(lop):
    _args = ()
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_instancemethod_args(lop):
    _args = (1, 2)
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_instancemethod_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_no_args(lop):
    _args = ()
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(Class())

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_args(lop):
    _args = (1, 2)
    _kwargs = {}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(Class(), *_args)

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(Class(), **_kwargs)

    assert result == (_args, _kwargs)


def test_instancemethod_via_class_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        def function(self, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(Class(), *_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_no_args(lop):
    _args = ()
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_classmethod_args(lop):
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_classmethod_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_via_class_no_args(lop):
    _args = ()
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_classmethod_via_class_args(lop):
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_classmethod_via_class_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_classmethod_via_class_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @classmethod
        def function(cls, *args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_no_args(lop):
    _args = ()
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_staticmethod_args(lop):
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_staticmethod_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class().function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_no_args(lop):
    _args = ()
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper()

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_args(lop):
    _args = (1, 2)
    _kwargs = {}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(*_args)

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_kwargs(lop):
    _args = ()
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(**_kwargs)

    assert result == (_args, _kwargs)


def test_staticmethod_via_class_args_plus_kwargs(lop):
    _args = (1, 2)
    _kwargs = {'one': 1, 'two': 2}

    class Class:
        @staticmethod
        def function(*args, **kwargs):
            return args, kwargs

    wrapper = lop.Proxy(lambda: Class.function)

    result = wrapper(*_args, **_kwargs)

    assert result == (_args, _kwargs)


def test_iteration(lop):
    items = [1, 2]

    wrapper = lop.Proxy(lambda: items)

    result = [x for x in wrapper]  # noqa: C416

    assert result == items

    with pytest.raises(TypeError):
        for _ in lop.Proxy(lambda: 1):
            pass


def test_iter_builtin(lop):
    iter(lop.Proxy(lambda: [1, 2]))
    pytest.raises(TypeError, iter, lop.Proxy(lambda: 1))


def test_context_manager(lop):
    class Class:
        def __enter__(self):
            return self

        def __exit__(*args, **kwargs):
            return

    instance = Class()

    wrapper = lop.Proxy(lambda: instance)

    with wrapper:
        pass


def test_object_hash(lop):
    def function1(*args, **kwargs):
        return args, kwargs

    function2 = lop.Proxy(lambda: function1)

    assert hash(function2) == hash(function1)


def test_mapping_key(lop):
    def function1(*args, **kwargs):
        return args, kwargs

    function2 = lop.Proxy(lambda: function1)

    table = {function1: True}

    assert table.get(function2)

    table = {function2: True}

    assert table.get(function1)


def test_comparison(lop):
    one = lop.Proxy(lambda: 1)
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert two > 1
    assert two >= 1
    assert two < 3
    assert two <= 3
    assert two != 1
    assert two == 2
    assert two != 3

    assert 2 > one
    assert 2 >= one
    assert 2 < three
    assert 2 <= three
    assert 2 != one
    assert 2 == two
    assert 2 != three

    assert two > one
    assert two >= one
    assert two < three
    assert two <= three
    assert two != one
    assert two == two
    assert two != three


def test_nonzero(lop):
    true = lop.Proxy(lambda: True)
    false = lop.Proxy(lambda: False)

    assert true
    assert not false

    assert bool(true)
    assert not bool(false)

    assert not false
    assert not not true


def test_int(lop):
    one = lop.Proxy(lambda: 1)

    assert int(one) == 1


def test_float(lop):
    one = lop.Proxy(lambda: 1)

    assert float(one) == 1.0


def test_add(lop):
    one = lop.Proxy(lambda: 1)
    two = lop.Proxy(lambda: 2)

    assert one + two == 1 + 2
    assert 1 + two == 1 + 2
    assert one + 2 == 1 + 2


def test_sub(lop):
    one = lop.Proxy(lambda: 1)
    two = lop.Proxy(lambda: 2)

    assert one - two == 1 - 2
    assert 1 - two == 1 - 2
    assert one - 2 == 1 - 2


def test_mul(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert two * three == 2 * 3
    assert 2 * three == 2 * 3
    assert two * 3 == 2 * 3


def test_matmul(lop):
    class MatmulClass:
        def __init__(self, value):
            self.value = value

        def __matmul__(self, other):
            return self.value * other.value

        def __rmatmul__(self, other):
            return other + self.value

    one = MatmulClass(123)
    two = MatmulClass(234)
    assert one @ two == 28782

    one = lop.Proxy(lambda: MatmulClass(123))
    two = lop.Proxy(lambda: MatmulClass(234))
    assert one @ two == 28782

    one = lop.Proxy(lambda: MatmulClass(123))
    two = MatmulClass(234)
    assert one @ two == 28782

    one = 123
    two = lop.Proxy(lambda: MatmulClass(234))
    assert one @ two == 357

    one = lop.Proxy(lambda: 123)
    two = lop.Proxy(lambda: MatmulClass(234))
    assert one @ two == 357


def test_div(lop):
    # On Python 2 this will pick up div and on Python
    # 3 it will pick up truediv.

    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert two / three == 2 / 3
    assert 2 / three == 2 / 3
    assert two / 3 == 2 / 3


def test_divdiv(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three // two == 3 // 2
    assert 3 // two == 3 // 2
    assert three // 2 == 3 // 2


def test_mod(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three % two == 3 % 2
    assert 3 % two == 3 % 2
    assert three % 2 == 3 % 2


def test_divmod(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert divmod(three, two), divmod(3 == 2)
    assert divmod(3, two), divmod(3 == 2)
    assert divmod(three, 2), divmod(3 == 2)


def test_pow(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three**two == pow(3, 2)
    assert 3**two == pow(3, 2)
    assert pow(3, two) == pow(3, 2)
    assert three**2 == pow(3, 2)

    assert pow(three, two) == pow(3, 2)
    assert pow(3, two) == pow(3, 2)
    assert pow(three, 2) == pow(3, 2)
    assert pow(three, 2, 2) == pow(3, 2, 2)


@pytest.mark.xfail
def test_pow_ternary(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert pow(three, two, 2) == pow(3, 2, 2)


@pytest.mark.xfail
def test_rpow_ternary(lop):
    two = lop.Proxy(lambda: 2)

    assert pow(3, two, 2) == pow(3, 2, 2)


def test_lshift(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three << two == 3 << 2
    assert 3 << two == 3 << 2
    assert three << 2 == 3 << 2


def test_rshift(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three >> two == 3 >> 2
    assert 3 >> two == 3 >> 2
    assert three >> 2 == 3 >> 2


def test_and(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three & two == 3 & 2
    assert 3 & two == 3 & 2
    assert three & 2 == 3 & 2


def test_xor(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three ^ two == 3 ^ 2
    assert 3 ^ two == 3 ^ 2
    assert three ^ 2 == 3 ^ 2


def test_or(lop):
    two = lop.Proxy(lambda: 2)
    three = lop.Proxy(lambda: 3)

    assert three | two == 3 | 2
    assert 3 | two == 3 | 2
    assert three | 2 == 3 | 2


def test_iadd(lop):
    value = lop.Proxy(lambda: 1)
    one = lop.Proxy(lambda: 1)

    value += 1
    assert value == 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value += one
    assert value == 3

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_isub(lop):
    value = lop.Proxy(lambda: 1)
    one = lop.Proxy(lambda: 1)

    value -= 1
    assert value == 0
    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value -= one
    assert value == -1

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_imul(lop):
    value = lop.Proxy(lambda: 2)
    two = lop.Proxy(lambda: 2)

    value *= 2
    assert value == 4

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value *= two
    assert value == 8

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_imatmul(lop):
    class InplaceMatmul:
        value = None

        def __imatmul__(self, other):
            self.value = other
            return self

    value = InplaceMatmul()
    assert value.value is None
    value @= 123
    assert value.value == 123

    value = lop.Proxy(InplaceMatmul)
    value @= 234
    assert value.value == 234

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_idiv(lop):
    # On Python 2 this will pick up div and on Python
    # 3 it will pick up truediv.

    value = lop.Proxy(lambda: 2)
    two = lop.Proxy(lambda: 2)

    value /= 2
    assert value == 2 / 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value /= two
    assert value == 2 / 2 / 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_ifloordiv(lop):
    value = lop.Proxy(lambda: 2)
    two = lop.Proxy(lambda: 2)

    value //= 2
    assert value == 2 // 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value //= two
    assert value == 2 // 2 // 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_imod(lop):
    value = lop.Proxy(lambda: 10)
    two = lop.Proxy(lambda: 2)

    value %= 2
    assert value == 10 % 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value %= two
    assert value == 10 % 2 % 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_ipow(lop):
    value = lop.Proxy(lambda: 10)
    two = lop.Proxy(lambda: 2)

    value **= 2
    assert value == 10**2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value **= two
    assert value == 10**2**2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_ilshift(lop):
    value = lop.Proxy(lambda: 256)
    two = lop.Proxy(lambda: 2)

    value <<= 2
    assert value == 256 << 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value <<= two
    assert value == 256 << 2 << 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_irshift(lop):
    value = lop.Proxy(lambda: 2)
    two = lop.Proxy(lambda: 2)

    value >>= 2
    assert value == 2 >> 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value >>= two
    assert value == 2 >> 2 >> 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_iand(lop):
    value = lop.Proxy(lambda: 1)
    two = lop.Proxy(lambda: 2)

    value &= 2
    assert value == 1 & 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value &= two
    assert value == 1 & 2 & 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_ixor(lop):
    value = lop.Proxy(lambda: 1)
    two = lop.Proxy(lambda: 2)

    value ^= 2
    assert value == 1 ^ 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value ^= two
    assert value == 1 ^ 2 ^ 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_ior(lop):
    value = lop.Proxy(lambda: 1)
    two = lop.Proxy(lambda: 2)

    value |= 2
    assert value == 1 | 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy

    value |= two
    assert value == 1 | 2 | 2

    if lop.kind != 'simple':
        assert type(value) is lop.Proxy


def test_neg(lop):
    value = lop.Proxy(lambda: 1)

    assert -value == -1


def test_pos(lop):
    value = lop.Proxy(lambda: 1)

    assert +value == 1


def test_abs(lop):
    value = lop.Proxy(lambda: -1)

    assert abs(value) == 1


def test_invert(lop):
    value = lop.Proxy(lambda: 1)

    assert ~value == ~1


def test_oct(lop):
    value = lop.Proxy(lambda: 20)

    assert oct(value) == oct(20)


def test_hex(lop):
    value = lop.Proxy(lambda: 20)

    assert hex(value) == hex(20)


def test_index(lop):
    class Class:
        def __index__(self):
            return 1

    value = lop.Proxy(lambda: Class())
    items = [0, 1, 2]

    assert items[value] == items[1]


def test_length(lop):
    value = lop.Proxy(lambda: list(range(3)))

    assert len(value) == 3


def test_contains(lop):
    value = lop.Proxy(lambda: list(range(3)))

    assert 2 in value
    assert -2 not in value


def test_getitem(lop):
    value = lop.Proxy(lambda: list(range(3)))

    assert value[1] == 1


def test_setitem(lop):
    value = lop.Proxy(lambda: list(range(3)))
    value[1] = -1

    assert value[1] == -1


def test_delitem(lop):
    value = lop.Proxy(lambda: list(range(3)))

    assert len(value) == 3

    del value[1]

    assert len(value) == 2
    assert value[1] == 2


def test_getslice(lop):
    value = lop.Proxy(lambda: list(range(5)))

    assert value[1:4] == [1, 2, 3]


def test_setslice(lop):
    value = lop.Proxy(lambda: list(range(5)))

    value[1:4] = reversed(value[1:4])

    assert value[1:4] == [3, 2, 1]


def test_delslice(lop):
    value = lop.Proxy(lambda: list(range(5)))

    del value[1:4]

    assert len(value) == 2
    assert value == [0, 4]


def test_dict_length(lop):
    value = lop.Proxy(lambda: dict.fromkeys(range(3), False))

    assert len(value) == 3


def test_dict_contains(lop):
    value = lop.Proxy(lambda: dict.fromkeys(range(3), False))

    assert 2 in value
    assert -2 not in value


def test_dict_getitem(lop):
    value = lop.Proxy(lambda: dict.fromkeys(range(3), False))

    assert value[1] is False


def test_dict_setitem(lop):
    value = lop.Proxy(lambda: dict.fromkeys(range(3), False))
    value[1] = True

    assert value[1] is True


def test_dict_delitem(lop):
    value = lop.Proxy(lambda: dict.fromkeys(range(3), False))

    assert len(value) == 3

    del value[1]

    assert len(value) == 2


def test_str(lop):
    value = lop.Proxy(lambda: 10)

    assert str(value) == str(10)

    value = lop.Proxy(lambda: (10,))

    assert str(value) == str((10,))

    value = lop.Proxy(lambda: [10])

    assert str(value) == str([10])

    value = lop.Proxy(lambda: {10: 10})

    assert str(value) == str({10: 10})


def test_repr(lop):
    class Foobar:
        pass

    value = lop.Proxy(lambda: Foobar())
    str(value)
    representation = repr(value)
    print(representation)
    assert 'Proxy at' in representation
    assert 'lambda' in representation
    assert 'Foobar' in representation


def test_repr_doesnt_consume(lop):
    consumed = []
    value = lop.Proxy(lambda: consumed.append(1))
    print(repr(value))
    assert not consumed


def test_derived_new(lop):
    class DerivedObjectProxy(lop.Proxy):
        def __new__(cls, wrapped):
            instance = super().__new__(cls)
            instance.__init__(wrapped)
            return instance

        def __init__(self, wrapped):
            super().__init__(wrapped)

    def function():
        return 123

    obj = DerivedObjectProxy(lambda: function)
    assert obj() == 123


def test_setup_class_attributes(lop):
    def function():
        pass

    class DerivedObjectProxy(lop.Proxy):
        pass

    obj = DerivedObjectProxy(lambda: function)

    DerivedObjectProxy.ATTRIBUTE = 1

    assert obj.ATTRIBUTE == 1
    assert not hasattr(function, 'ATTRIBUTE')

    del DerivedObjectProxy.ATTRIBUTE

    assert not hasattr(DerivedObjectProxy, 'ATTRIBUTE')
    assert not hasattr(obj, 'ATTRIBUTE')
    assert not hasattr(function, 'ATTRIBUTE')


def test_override_class_attributes(lop):
    def function():
        pass

    class DerivedObjectProxy(lop.Proxy):
        ATTRIBUTE = 1

    obj = DerivedObjectProxy(lambda: function)

    assert DerivedObjectProxy.ATTRIBUTE == 1
    assert obj.ATTRIBUTE == 1

    obj.ATTRIBUTE = 2

    assert DerivedObjectProxy.ATTRIBUTE == 1

    assert obj.ATTRIBUTE == 2
    assert not hasattr(function, 'ATTRIBUTE')

    del DerivedObjectProxy.ATTRIBUTE

    assert not hasattr(DerivedObjectProxy, 'ATTRIBUTE')
    assert obj.ATTRIBUTE == 2
    assert not hasattr(function, 'ATTRIBUTE')


def test_attr_functions(lop):
    def function():
        pass

    proxy = lop.Proxy(lambda: function)

    assert hasattr(proxy, '__getattr__')
    assert hasattr(proxy, '__setattr__')
    assert hasattr(proxy, '__delattr__')


def test_override_getattr(lop):
    def function():
        pass

    accessed = []

    class DerivedObjectProxy(lop.Proxy):
        def __getattr__(self, name):
            accessed.append(name)
            try:
                __getattr__ = super().__getattr__
            except AttributeError as e:
                raise RuntimeError(str(e)) from e
            return __getattr__(name)

    function.attribute = 1

    proxy = DerivedObjectProxy(lambda: function)

    assert proxy.attribute == 1

    assert 'attribute' in accessed


skipcallable = pytest.mark.xfail(reason="Don't know how to make this work. This tests the existence of the __call__ method.")


@skipcallable
def test_proxy_hasattr_call(lop):
    proxy = lop.Proxy(lambda: None)

    assert not callable(proxy)


@skipcallable
def test_proxy_getattr_call(lop):
    proxy = lop.Proxy(lambda: None)

    assert getattr(proxy, '__call__', None) is None


@skipcallable
def test_proxy_is_callable(lop):
    proxy = lop.Proxy(lambda: None)

    assert not callable(proxy)


def test_callable_proxy_hasattr_call(lop):
    proxy = lop.Proxy(lambda: None)

    assert callable(proxy)


@skipcallable
def test_callable_proxy_getattr_call(lop):
    proxy = lop.Proxy(lambda: None)

    assert getattr(proxy, '__call__', None) is None


def test_callable_proxy_is_callable(lop):
    proxy = lop.Proxy(lambda: None)

    assert callable(proxy)


def test_class_bytes(lop):
    class Class:
        def __bytes__(self):
            return b'BYTES'

    instance = Class()

    proxy = lop.Proxy(lambda: instance)

    assert bytes(instance) == bytes(proxy)


def test_str_format(lop):
    instance = 'abcd'

    proxy = lop.Proxy(lambda: instance)

    assert format(instance, ''), format(proxy == '')


def test_list_reversed(lop):
    instance = [1, 2]

    proxy = lop.Proxy(lambda: instance)

    assert list(reversed(instance)) == list(reversed(proxy))


def test_decimal_complex(lop):
    import decimal

    instance = decimal.Decimal(123)

    proxy = lop.Proxy(lambda: instance)

    assert complex(instance) == complex(proxy)


def test_fractions_round(lop):
    import fractions

    instance = fractions.Fraction('1/2')

    proxy = lop.Proxy(lambda: instance)

    assert round(instance) == round(proxy)


def test_readonly(lop):
    proxy = lop.Proxy(lambda: object)
    assert proxy.__qualname__ == 'object'


def test_del_wrapped(lop):
    foo = object()
    called = []

    def make_foo():
        called.append(1)
        return foo

    proxy = lop.Proxy(make_foo)
    str(proxy)
    assert called == [1]
    assert proxy.__wrapped__ is foo
    # print(type(proxy), hasattr(type(proxy), '__wrapped__'))
    del proxy.__wrapped__
    str(proxy)
    assert called == [1, 1]


def test_raise_attribute_error(lop):
    def foo():
        raise AttributeError('boom!')

    proxy = lop.Proxy(foo)
    pytest.raises(AttributeError, str, proxy)
    pytest.raises(AttributeError, lambda: proxy.__wrapped__)
    assert proxy.__factory__ is foo


def test_patching_the_factory(lop):
    def foo():
        raise AttributeError('boom!')

    proxy = lop.Proxy(foo)
    pytest.raises(AttributeError, lambda: proxy.__wrapped__)
    assert proxy.__factory__ is foo

    proxy.__factory__ = lambda: foo
    pytest.raises(AttributeError, proxy)
    assert proxy.__wrapped__ is foo


def test_deleting_the_factory(lop):
    proxy = lop.Proxy(None)
    assert proxy.__factory__ is None
    proxy.__factory__ = None
    assert proxy.__factory__ is None

    pytest.raises(TypeError, str, proxy)
    del proxy.__factory__
    pytest.raises(ValueError, str, proxy)


def test_patching_the_factory_with_none(lop):
    proxy = lop.Proxy(None)
    assert proxy.__factory__ is None
    proxy.__factory__ = None
    assert proxy.__factory__ is None
    proxy.__factory__ = None
    assert proxy.__factory__ is None

    def foo():
        return 1

    proxy.__factory__ = foo
    assert proxy.__factory__ is foo
    assert proxy.__wrapped__ == 1
    assert str(proxy) == '1'


def test_new(lop):
    a = lop.Proxy.__new__(lop.Proxy)
    b = lop.Proxy.__new__(lop.Proxy)
    # NOW KISS
    pytest.raises(ValueError, lambda: a + b)
    # no segfault, yay
    pytest.raises(ValueError, lambda: a.__wrapped__)


def test_set_wrapped_via_new(lop):
    obj = lop.Proxy.__new__(lop.Proxy)
    obj.__wrapped__ = 1
    assert str(obj) == '1'
    assert obj + 1 == 2


def test_set_wrapped_regular(lop):
    obj = lop.Proxy(None)
    obj.__wrapped__ = 1
    assert str(obj) == '1'
    assert obj + 1 == 2


@pytest.fixture(
    params=[
        'pickle',
    ]
)
def pickler(request):
    return pytest.importorskip(request.param)


@pytest.mark.parametrize('obj', [1, 1.2, 'a', ['b', 'c'], {'d': 'e'}, date(2015, 5, 1), datetime(2015, 5, 1), Decimal('1.2')])
@pytest.mark.parametrize('level', range(pickle.HIGHEST_PROTOCOL + 1))
def test_pickling(lop, obj, pickler, level):
    proxy = lop.Proxy(lambda: obj)
    dump = pickler.dumps(proxy, protocol=level)
    result = pickler.loads(dump)
    assert obj == result


@pytest.mark.parametrize('level', range(pickle.HIGHEST_PROTOCOL + 1))
def test_pickling_exception(lop, pickler, level):
    class BadStuff(Exception):
        pass

    def trouble_maker():
        raise BadStuff('foo')

    proxy = lop.Proxy(trouble_maker)
    pytest.raises(BadStuff, pickler.dumps, proxy, protocol=level)


@pytest.mark.skipif(platform.python_implementation() != 'CPython', reason="Interpreter doesn't have reference counting")
def test_garbage_collection(lop):
    leaky = lambda: 'foobar'  # noqa
    proxy = lop.Proxy(leaky)
    leaky.leak = proxy
    ref = weakref.ref(leaky)
    assert proxy == 'foobar'
    del leaky
    del proxy
    gc.collect()
    assert ref() is None


@pytest.mark.skipif(platform.python_implementation() != 'CPython', reason="Interpreter doesn't have reference counting")
def test_garbage_collection_count(lop):
    obj = object()
    count = sys.getrefcount(obj)
    for _ in range(100):
        str(lop.Proxy(lambda: obj))
    assert count == sys.getrefcount(obj)


@pytest.mark.parametrize('name', ['slots', 'cext', 'simple', 'django', 'objproxies'])
@pytest.mark.skip('Too much deps required.')
def test_perf(benchmark, name, lop_loader):
    implementation = lop_loader(name)
    obj = 'foobar'
    proxied = implementation.Proxy(lambda: obj)
    assert benchmark(partial(str, proxied)) == obj


empty = object()


@pytest.fixture(scope='module', params=['SimpleProxy', 'LocalsSimpleProxy', 'CachedPropertyProxy', 'LocalsCachedPropertyProxy'])
def prototype(request):
    from lazy_object_proxy.simple import cached_property

    name = request.param

    if name == 'SimpleProxy':

        class SimpleProxy:
            def __init__(self, factory):
                self.factory = factory
                self.object = empty

            def __str__(self):
                if self.object is empty:
                    self.object = self.factory()
                return str(self.object)

        return SimpleProxy
    elif name == 'CachedPropertyProxy':

        class CachedPropertyProxy:
            def __init__(self, factory):
                self.factory = factory

            @cached_property
            def object(self):
                return self.factory()

            def __str__(self):
                return str(self.object)

        return CachedPropertyProxy
    elif name == 'LocalsSimpleProxy':

        class LocalsSimpleProxy:
            def __init__(self, factory):
                self.factory = factory
                self.object = empty

            def __str__(self, func=str):
                if self.object is empty:
                    self.object = self.factory()
                return func(self.object)

        return LocalsSimpleProxy
    elif name == 'LocalsCachedPropertyProxy':

        class LocalsCachedPropertyProxy:
            def __init__(self, factory):
                self.factory = factory

            @cached_property
            def object(self):
                return self.factory()

            def __str__(self, func=str):
                return func(self.object)

        return LocalsCachedPropertyProxy


@pytest.mark.benchmark(group='prototypes')
@pytest.mark.skip('Too much deps required.')
def test_proto(benchmark, prototype):
    obj = 'foobar'
    proxied = prototype(lambda: obj)
    assert benchmark(partial(str, proxied)) == obj


def test_subclassing_with_local_attr(lop):
    class Foo:
        pass

    called = []

    class LazyProxy(lop.Proxy):
        name = None

        def __init__(self, func, **lazy_attr):
            super().__init__(func)
            for attr, val in lazy_attr.items():
                setattr(self, attr, val)

    proxy = LazyProxy(lambda: called.append(1) or Foo(), name='bar')
    assert proxy.name == 'bar'
    assert not called


def test_subclassing_dynamic_with_local_attr(lop):
    if lop.kind == 'cext':
        pytest.skip('Not possible.')

    class Foo:
        pass

    called = []

    class LazyProxy(lop.Proxy):
        def __init__(self, func, **lazy_attr):
            super().__init__(func)
            for attr, val in lazy_attr.items():
                object.__setattr__(self, attr, val)

    proxy = LazyProxy(lambda: called.append(1) or Foo(), name='bar')
    assert proxy.name == 'bar'
    assert not called


class FSPathMock:
    def __fspath__(self):
        return '/foobar'


@pytest.mark.skipif(not hasattr(os, 'fspath'), reason='No os.fspath support.')
def test_fspath(lop):
    assert os.fspath(lop.Proxy(lambda: '/foobar')) == '/foobar'
    assert os.fspath(lop.Proxy(FSPathMock)) == '/foobar'
    with pytest.raises(TypeError) as excinfo:
        os.fspath(lop.Proxy(lambda: None))
    assert '__fspath__() to return str or bytes, not NoneType' in excinfo.value.args[0]


def test_fspath_method(lop):
    assert lop.Proxy(FSPathMock).__fspath__() == '/foobar'


def test_resolved_new(lop):
    obj = lop.Proxy.__new__(lop.Proxy)
    assert obj.__resolved__ is False


def test_resolved(lop):
    obj = lop.Proxy(lambda: None)
    assert obj.__resolved__ is False
    assert obj.__wrapped__ is None
    assert obj.__resolved__ is True


def test_resolved_str(lop):
    obj = lop.Proxy(lambda: None)
    assert obj.__resolved__ is False
    str(obj)
    assert obj.__resolved__ is True


def test_format(lop):
    class WithFormat:
        def __format__(self, format_spec):
            return f'spec({format_spec!r})'

    obj = lop.Proxy(WithFormat)
    assert f'{obj:stuff}' == "spec('stuff')"
