from textwrap import dedent

import pytest

from datadiff import tools


def test_assert_equal_true():
    # nothing raised
    assert None is tools.assert_equals(7, 7)


def test_assert_equal_false():
    with pytest.raises(AssertionError) as raised:
        tools.assert_equals([3, 4], [5, 6])
    assert str(raised.value) == dedent('''\
        
        --- a
        +++ b
        [
        @@ -0,1 +0,1 @@
        -3,
        -4,
        +5,
        +6,
        ]''')


def test_assert_equal_msg():
    with pytest.raises(AssertionError) as raised:
        tools.assert_equals(3, 4, "whoops")
    assert str(raised.value) == "whoops"


def test_assert_equals():
    assert tools.assert_equal == tools.assert_equals


def test_assert_equal_simple():
    with pytest.raises(AssertionError) as raised:
        tools.assert_equals(True, False)
    assert str(raised.value) == 'True != False'


def test_assert_equal_simple_types():
    with pytest.raises(AssertionError) as raised:
        tools.assert_equals('a', 7)
    assert str(raised.value) == dedent("'a' != 7")


def test_assert_almost_equal():
    tools.assertAlmostEqual([1.0], [1.0])
    tools.assertAlmostEqual([1.0], [1.000000001])
    tools.assertAlmostEqual([1.0], [1.00001], places=4)
    tools.assertAlmostEqual({"k": 1.0}, {"k": 1.00001}, places=4)
    tools.assertAlmostEqual({1.0}, {1.00001}, places=4)


def test_assert_not_almost_equal():
    pytest.raises(AssertionError, tools.assertAlmostEqual, [1.0], [1.00001])
    pytest.raises(AssertionError, tools.assertAlmostEqual, [1.0], [1.0001], places=4)
    pytest.raises(AssertionError, tools.assertAlmostEqual, {"k": 1.0}, {"k": 1.1}, places=4)
    pytest.raises(AssertionError, tools.assertAlmostEqual, {1.0}, {1.1}, places=4)
