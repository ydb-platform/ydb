import platform
import pprint
from functools import singledispatch

import packaging.version
import pytest

from dirty_equals import Contains, DirtyEquals, IsApprox, IsInt, IsList, IsNegative, IsOneOf, IsPositive, IsStr
from dirty_equals.version import VERSION


def test_or():
    assert 'foo' == IsStr | IsInt
    assert 1 == IsStr | IsInt
    assert -1 == IsStr | IsNegative | IsPositive

    v = IsStr | IsInt
    with pytest.raises(AssertionError):
        assert 1.5 == v
    assert str(v) == 'IsStr | IsInt'


def test_and():
    assert 4 == IsPositive & IsInt(lt=5)

    v = IsStr & IsInt
    with pytest.raises(AssertionError):
        assert 1 == v
    assert str(v) == 'IsStr & IsInt'


def test_not():
    assert 'foo' != IsInt
    assert 'foo' == ~IsInt


def test_value_eq():
    v = IsStr()

    with pytest.raises(AttributeError, match='value is not available until __eq__ has been called'):
        v.value

    assert 'foo' == v
    assert repr(v) == str(v) == "'foo'" == pprint.pformat(v)
    assert v.value == 'foo'


def test_value_ne():
    v = IsStr()

    with pytest.raises(AssertionError):
        assert 1 == v

    assert repr(v) == str(v) == 'IsStr()' == pprint.pformat(v)
    with pytest.raises(AttributeError, match='value is not available until __eq__ has been called'):
        v.value


def test_dict_compare():
    v = {'foo': 1, 'bar': 2, 'spam': 3}
    assert v == {'foo': IsInt, 'bar': IsPositive, 'spam': ~IsStr}
    assert v == {'foo': IsInt() & IsApprox(1), 'bar': IsPositive() | IsNegative(), 'spam': ~IsStr()}


@pytest.mark.skipif(platform.python_implementation() == 'PyPy', reason='PyPy does not metaclass dunder methods')
def test_not_repr():
    v = ~IsInt
    assert str(v) == '~IsInt'

    with pytest.raises(AssertionError):
        assert 1 == v

    assert str(v) == '~IsInt'


def test_not_repr_instance():
    v = ~IsInt()
    assert str(v) == '~IsInt()'

    with pytest.raises(AssertionError):
        assert 1 == v

    assert str(v) == '~IsInt()'


def test_repr():
    v = ~IsInt
    assert str(v) == '~IsInt'

    assert '1' == v

    assert str(v) == "'1'"


@pytest.mark.parametrize(
    'v,v_repr',
    [
        (IsInt, 'IsInt'),
        (~IsInt, '~IsInt'),
        (IsInt & IsPositive, 'IsInt & IsPositive'),
        (IsInt | IsPositive, 'IsInt | IsPositive'),
        (IsInt(), 'IsInt()'),
        (~IsInt(), '~IsInt()'),
        (IsInt() & IsPositive(), 'IsInt() & IsPositive()'),
        (IsInt() | IsPositive(), 'IsInt() | IsPositive()'),
        (IsInt() & IsPositive, 'IsInt() & IsPositive'),
        (IsInt() | IsPositive, 'IsInt() | IsPositive'),
        (IsPositive & IsInt(lt=5), 'IsPositive & IsInt(lt=5)'),
        (IsOneOf(1, 2, 3), 'IsOneOf(1, 2, 3)'),
    ],
)
def test_repr_class(v, v_repr):
    assert repr(v) == str(v) == v_repr == pprint.pformat(v)


def test_is_approx_without_init():
    assert 1 != IsApprox


def test_ne_repr():
    v = IsInt
    assert repr(v) == str(v) == 'IsInt' == pprint.pformat(v)

    assert 'x' != v

    assert repr(v) == str(v) == 'IsInt' == pprint.pformat(v)


def test_pprint():
    v = [IsList(length=...), 1, [IsList(length=...), 2], 3, IsInt()]
    lorem = ['lorem', 'ipsum', 'dolor', 'sit', 'amet'] * 2
    with pytest.raises(AssertionError):
        assert [lorem, 1, [lorem, 2], 3, '4'] == v

    assert repr(v) == (f'[{lorem}, 1, [{lorem}, 2], 3, IsInt()]')
    assert pprint.pformat(v) == (
        "[['lorem',\n"
        "  'ipsum',\n"
        "  'dolor',\n"
        "  'sit',\n"
        "  'amet',\n"
        "  'lorem',\n"
        "  'ipsum',\n"
        "  'dolor',\n"
        "  'sit',\n"
        "  'amet'],\n"
        ' 1,\n'
        " [['lorem',\n"
        "   'ipsum',\n"
        "   'dolor',\n"
        "   'sit',\n"
        "   'amet',\n"
        "   'lorem',\n"
        "   'ipsum',\n"
        "   'dolor',\n"
        "   'sit',\n"
        "   'amet'],\n"
        '  2],\n'
        ' 3,\n'
        ' IsInt()]'
    )


def test_pprint_not_equal():
    v = IsList(*range(30))  # need a big value to trigger pprint
    with pytest.raises(AssertionError):
        assert [] == v

    assert (
        pprint.pformat(v)
        == (
            'IsList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, '
            '15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)'
        )
        == repr(v)
        == str(v)
    )


@pytest.mark.parametrize(
    'value,dirty',
    [
        (1, IsOneOf(1, 2, 3)),
        (4, ~IsOneOf(1, 2, 3)),
        ([1, 2, 3], Contains(1) | IsOneOf([])),
        ([], Contains(1) | IsOneOf([])),
        ([2], ~(Contains(1) | IsOneOf([]))),
    ],
)
def test_is_one_of(value, dirty):
    assert value == dirty


def test_version():
    packaging.version.parse(VERSION)


def test_singledispatch():
    @singledispatch
    def dispatch(value):
        return 'generic'

    assert dispatch(IsStr()) == 'generic'

    @dispatch.register
    def _(value: DirtyEquals):
        return 'DirtyEquals'

    assert dispatch(IsStr()) == 'DirtyEquals'

    @dispatch.register
    def _(value: IsStr):
        return 'IsStr'

    assert dispatch(IsStr()) == 'IsStr'
