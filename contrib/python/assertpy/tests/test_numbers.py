# Copyright (c) 2015-2019, Activision Publishing, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import math

from assertpy import assert_that, fail


def test_is_zero():
    assert_that(0).is_zero()
    # assert_that(0L).is_zero()
    assert_that(0.0).is_zero()
    assert_that(0 + 0j).is_zero()


def test_is_zero_failure():
    try:
        assert_that(1).is_zero()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <1> to be equal to <0>, but was not.')


def test_is_zero_bad_type_failure():
    try:
        assert_that('foo').is_zero()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric')


def test_is_not_zero():
    assert_that(1).is_not_zero()
    # assert_that(1L).is_not_zero()
    assert_that(0.001).is_not_zero()
    assert_that(0 + 1j).is_not_zero()


def test_is_not_zero_failure():
    try:
        assert_that(0).is_not_zero()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <0> to be not equal to <0>, but was.')


def test_is_not_zero_bad_type_failure():
    try:
        assert_that('foo').is_not_zero()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric')


def test_is_nan():
    assert_that(float('NaN')).is_nan()
    assert_that(float('Inf')-float('Inf')).is_nan()


def test_is_nan_failure():
    try:
        assert_that(0).is_nan()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <0> to be <NaN>, but was not.')


def test_is_nan_bad_type_failure():
    try:
        assert_that('foo').is_nan()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric')


def test_is_nan_bad_type_failure_complex():
    try:
        assert_that(1 + 2j).is_nan()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not real number')


def test_is_not_nan():
    assert_that(1).is_not_nan()
    assert_that(1.0).is_not_nan()


def test_is_not_nan_failure():
    try:
        assert_that(float('NaN')).is_not_nan()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected not <NaN>, but was.')


def test_is_not_nan_bad_type_failure():
    try:
        assert_that('foo').is_not_nan()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric')


def test_is_not_nan_bad_type_failure_complex():
    try:
        assert_that(1 + 2j).is_not_nan()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not real number')


def test_is_inf():
    assert_that(float('Inf')).is_inf()
    assert_that(1e1000).is_inf()


def test_is_inf_failure():
    try:
        assert_that(0).is_inf()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <0> to be <Inf>, but was not.')


def test_is_inf_bad_type_failure():
    try:
        assert_that('foo').is_inf()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric')


def test_is_inf_bad_type_failure_complex():
    try:
        assert_that(1 + 2j).is_inf()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not real number')


def test_is_not_inf():
    assert_that(1).is_not_inf()
    assert_that(123.456).is_not_inf()


def test_is_not_inf_failure():
    try:
        assert_that(float('Inf')).is_not_inf()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected not <Inf>, but was.')


def test_is_not_inf_bad_type_failure():
    try:
        assert_that('foo').is_not_inf()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric')


def test_is_not_inf_bad_type_failure_complex():
    try:
        assert_that(1 + 2j).is_not_inf()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not real number')


def test_is_greater_than():
    assert_that(123).is_greater_than(100)
    assert_that(123).is_greater_than(0)
    assert_that(123).is_greater_than(-100)
    assert_that(123).is_greater_than(122.5)


def test_is_greater_than_failure():
    try:
        assert_that(123).is_greater_than(123)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to be greater than <123>, but was not.')


def test_is_greater_than_complex_failure():
    try:
        assert_that(1 + 2j).is_greater_than(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <complex>')


def test_is_greater_than_bad_value_type_failure():
    try:
        assert_that('foo').is_greater_than(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <str>')


def test_is_greater_than_bad_arg_type_failure():
    try:
        assert_that(123).is_greater_than('foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a number, but was <str>')


def test_is_greater_than_or_equal_to():
    assert_that(123).is_greater_than_or_equal_to(100)
    assert_that(123).is_greater_than_or_equal_to(123)
    assert_that(123).is_greater_than_or_equal_to(0)
    assert_that(123).is_greater_than_or_equal_to(-100)
    assert_that(123).is_greater_than_or_equal_to(122.5)


def test_is_greater_than_or_equal_to_failure():
    try:
        assert_that(123).is_greater_than_or_equal_to(1000)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to be greater than or equal to <1000>, but was not.')


def test_is_greater_than_or_equal_to_complex_failure():
    try:
        assert_that(1 + 2j).is_greater_than_or_equal_to(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <complex>')


def test_is_greater_than_or_equal_to_bad_value_type_failure():
    try:
        assert_that('foo').is_greater_than_or_equal_to(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <str>')


def test_is_greater_than_or_equal_to_bad_arg_type_failure():
    try:
        assert_that(123).is_greater_than_or_equal_to('foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a number, but was <str>')


def test_is_less_than():
    assert_that(123).is_less_than(1000)
    assert_that(123).is_less_than(1e6)
    assert_that(-123).is_less_than(-100)
    assert_that(123).is_less_than(123.001)


def test_is_less_than_failure():
    try:
        assert_that(123).is_less_than(123)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to be less than <123>, but was not.')


def test_is_less_than_complex_failure():
    try:
        assert_that(1 + 2j).is_less_than(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <complex>')


def test_is_less_than_bad_value_type_failure():
    try:
        assert_that('foo').is_less_than(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <str>')


def test_is_less_than_bad_arg_type_failure():
    try:
        assert_that(123).is_less_than('foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a number, but was <str>')


def test_is_less_than_or_equal_to():
    assert_that(123).is_less_than_or_equal_to(1000)
    assert_that(123).is_less_than_or_equal_to(123)
    assert_that(123).is_less_than_or_equal_to(1e6)
    assert_that(-123).is_less_than_or_equal_to(-100)
    assert_that(123).is_less_than_or_equal_to(123.001)


def test_is_less_than_or_equal_to_failure():
    try:
        assert_that(123).is_less_than_or_equal_to(100)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to be less than or equal to <100>, but was not.')


def test_is_less_than_or_equal_to_complex_failure():
    try:
        assert_that(1 + 2j).is_less_than_or_equal_to(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <complex>')


def test_is_less_than_or_equal_to_bad_value_type_failure():
    try:
        assert_that('foo').is_less_than_or_equal_to(0)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <str>')


def test_is_less_than_or_equal_to_bad_arg_type_failure():
    try:
        assert_that(123).is_less_than_or_equal_to('foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a number, but was <str>')


def test_is_positive():
    assert_that(1).is_positive()


def test_is_positive_failure():
    try:
        assert_that(0).is_positive()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <0> to be greater than <0>, but was not.')


def test_is_negative():
    assert_that(-1).is_negative()


def test_is_negative_failure():
    try:
        assert_that(0).is_negative()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <0> to be less than <0>, but was not.')


def test_is_between():
    assert_that(123).is_between(120, 125)
    assert_that(123).is_between(0, 1e6)
    assert_that(-123).is_between(-150, -100)
    assert_that(123).is_between(122.999, 123.001)


def test_is_between_failure():
    try:
        assert_that(123).is_between(0, 1)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to be between <0> and <1>, but was not.')


def test_is_between_complex_failure():
    try:
        assert_that(1 + 2j).is_between(0, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <complex>')


def test_is_between_bad_value_type_failure():
    try:
        assert_that('foo').is_between(0, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <str>')


def test_is_between_low_arg_type_failure():
    try:
        assert_that(123).is_between('foo', 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given low arg must be numeric, but was <str>')


def test_is_between_high_arg_type_failure():
    try:
        assert_that(123).is_between(0, 'foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given high arg must be numeric, but was <str>')


def test_is_between_bad_arg_delta_failure():
    try:
        assert_that(123).is_between(1, 0)
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given low arg must be less than given high arg')


def test_is_not_between():
    assert_that(123).is_not_between(124, 125)
    assert_that(123).is_not_between(1e5, 1e6)
    assert_that(-123).is_not_between(-1000, -150)
    assert_that(123).is_not_between(122.999, 122.9999)


def test_is_not_between_failure():
    try:
        assert_that(123).is_not_between(0, 1000)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to not be between <0> and <1000>, but was.')


def test_is_not_between_complex_failure():
    try:
        assert_that(1 + 2j).is_not_between(0, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <complex>')


def test_is_not_between_bad_value_type_failure():
    try:
        assert_that('foo').is_not_between(0, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for type <str>')


def test_is_not_between_low_arg_type_failure():
    try:
        assert_that(123).is_not_between('foo', 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given low arg must be numeric, but was <str>')


def test_is_not_between_high_arg_type_failure():
    try:
        assert_that(123).is_not_between(0, 'foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given high arg must be numeric, but was <str>')


def test_is_not_between_bad_arg_delta_failure():
    try:
        assert_that(123).is_not_between(1, 0)
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given low arg must be less than given high arg')


def test_is_close_to():
    assert_that(123.01).is_close_to(123, 1)
    assert_that(0.01).is_close_to(0, 1)
    assert_that(-123.01).is_close_to(-123, 1)


def test_is_close_to_failure():
    try:
        assert_that(123.01).is_close_to(100, 1)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123.01> to be close to <100> within tolerance <1>, but was not.')


def test_is_close_to_complex_failure():
    try:
        assert_that(1 + 2j).is_close_to(0, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for complex numbers')


def test_is_close_to_bad_value_type_failure():
    try:
        assert_that('foo').is_close_to(123, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric or datetime')


def test_is_close_to_bad_arg_type_failure():
    try:
        assert_that(123.01).is_close_to('foo', 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be numeric')


def test_is_close_to_bad_tolerance_arg_type_failure():
    try:
        assert_that(123.01).is_close_to(0, 'foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given tolerance arg must be numeric')


def test_is_close_to_negative_tolerance_failure():
    try:
        assert_that(123.01).is_close_to(123, -1)
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given tolerance arg must be positive')


def test_is_not_close_to():
    assert_that(123.01).is_not_close_to(122, 1)
    assert_that(0.01).is_not_close_to(0, 0.001)
    assert_that(-123.01).is_not_close_to(-122, 1)


def test_is_not_close_to_failure():
    try:
        assert_that(123.01).is_not_close_to(123, 1)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123.01> to not be close to <123> within tolerance <1>, but was.')


def test_is_not_close_to_complex_failure():
    try:
        assert_that(1 + 2j).is_not_close_to(0, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('ordering is not defined for complex numbers')


def test_is_not_close_to_bad_value_type_failure():
    try:
        assert_that('foo').is_not_close_to(123, 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not numeric or datetime')


def test_is_not_close_to_bad_arg_type_failure():
    try:
        assert_that(123.01).is_not_close_to('foo', 1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be numeric')


def test_is_not_close_to_bad_tolerance_arg_type_failure():
    try:
        assert_that(123.01).is_not_close_to(0, 'foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given tolerance arg must be numeric')


def test_is_not_close_to_negative_tolerance_failure():
    try:
        assert_that(123.01).is_not_close_to(123, -1)
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given tolerance arg must be positive')


def test_chaining():
    assert_that(123).is_greater_than(100).is_less_than(1000).is_between(120, 125).is_close_to(100, 25)
