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

import sys
import os
import datetime
from assertpy import assert_that, assert_warn, soft_assertions, contents_of, fail


def setup_module():
    print('\nTEST test_readme.py : v%d.%d.%d' % (sys.version_info[0], sys.version_info[1], sys.version_info[2]))


def test_something():
    assert_that(1 + 2).is_equal_to(3)
    assert_that('foobar').is_length(6).starts_with('foo').ends_with('bar')
    assert_that(['a', 'b', 'c']).contains('a').does_not_contain('x')


def test_strings():
    assert_that('').is_not_none()
    assert_that('').is_empty()
    assert_that('').is_false()
    assert_that('').is_type_of(str)
    assert_that('').is_instance_of(str)

    assert_that('foo').is_length(3)
    assert_that('foo').is_not_empty()
    assert_that('foo').is_true()
    assert_that('foo').is_alpha()
    assert_that('123').is_digit()
    assert_that('foo').is_lower()
    assert_that('FOO').is_upper()
    assert_that('foo').is_iterable()
    assert_that('foo').is_equal_to('foo')
    assert_that('foo').is_not_equal_to('bar')
    assert_that('foo').is_equal_to_ignoring_case('FOO')

    if sys.version_info[0] == 3:
        assert_that('foo').is_unicode()
    else:
        assert_that(u'foo').is_unicode()

    assert_that('foo').contains('f')
    assert_that('foo').contains('f', 'oo')
    assert_that('foo').contains_ignoring_case('F', 'oO')
    assert_that('foo').does_not_contain('x')
    assert_that('foo').contains_only('f', 'o')
    assert_that('foo').contains_sequence('o', 'o')

    assert_that('foo').contains_duplicates()
    assert_that('fox').does_not_contain_duplicates()

    assert_that('foo').is_in('foo', 'bar', 'baz')
    assert_that('foo').is_not_in('boo', 'bar', 'baz')
    assert_that('foo').is_subset_of('abcdefghijklmnopqrstuvwxyz')

    assert_that('foo').starts_with('f')
    assert_that('foo').ends_with('oo')

    assert_that('foo').matches(r'\w')
    assert_that('123-456-7890').matches(r'\d{3}-\d{3}-\d{4}')
    assert_that('foo').does_not_match(r'\d+')

    # partial matches, these all pass
    assert_that('foo').matches(r'\w')
    assert_that('foo').matches(r'oo')
    assert_that('foo').matches(r'\w{2}')

    # match the entire string with an anchored regex pattern, passes
    assert_that('foo').matches(r'^\w{3}$')

    # fails
    try:
        assert_that('foo').matches(r'^\w{2}$')
        fail('should have raised error')
    except AssertionError:
        pass


def test_ints():
    assert_that(0).is_not_none()
    assert_that(0).is_false()
    assert_that(0).is_type_of(int)
    assert_that(0).is_instance_of(int)

    assert_that(0).is_zero()
    assert_that(1).is_not_zero()
    assert_that(1).is_positive()
    assert_that(-1).is_negative()

    assert_that(123).is_equal_to(123)
    assert_that(123).is_not_equal_to(456)

    assert_that(123).is_greater_than(100)
    assert_that(123).is_greater_than_or_equal_to(123)
    assert_that(123).is_less_than(200)
    assert_that(123).is_less_than_or_equal_to(200)
    assert_that(123).is_between(100, 200)
    assert_that(123).is_close_to(100, 25)

    assert_that(1).is_in(0, 1, 2, 3)
    assert_that(1).is_not_in(-1, -2, -3)


def test_floats():
    assert_that(0.0).is_not_none()
    assert_that(0.0).is_false()
    assert_that(0.0).is_type_of(float)
    assert_that(0.0).is_instance_of(float)

    assert_that(123.4).is_equal_to(123.4)
    assert_that(123.4).is_not_equal_to(456.7)

    assert_that(123.4).is_greater_than(100.1)
    assert_that(123.4).is_greater_than_or_equal_to(123.4)
    assert_that(123.4).is_less_than(200.2)
    assert_that(123.4).is_less_than_or_equal_to(123.4)
    assert_that(123.4).is_between(100.1, 200.2)
    assert_that(123.4).is_close_to(123, 0.5)

    assert_that(float('NaN')).is_nan()
    assert_that(123.4).is_not_nan()
    assert_that(float('Inf')).is_inf()
    assert_that(123.4).is_not_inf()


def test_lists():
    assert_that([]).is_not_none()
    assert_that([]).is_empty()
    assert_that([]).is_false()
    assert_that([]).is_type_of(list)
    assert_that([]).is_instance_of(list)
    assert_that([]).is_iterable()

    assert_that(['a', 'b']).is_length(2)
    assert_that(['a', 'b']).is_not_empty()
    assert_that(['a', 'b']).is_equal_to(['a', 'b'])
    assert_that(['a', 'b']).is_not_equal_to(['b', 'a'])

    assert_that(['a', 'b']).contains('a')
    assert_that(['a', 'b']).contains('b', 'a')
    assert_that(['a', 'b']).does_not_contain('x', 'y')
    assert_that(['a', 'b']).contains_only('a', 'b')
    assert_that(['a', 'a']).contains_only('a')
    assert_that(['a', 'b', 'c']).contains_sequence('b', 'c')
    assert_that(['a', 'b']).is_subset_of(['a', 'b', 'c'])
    assert_that(['a', 'b', 'c']).is_sorted()
    assert_that(['c', 'b', 'a']).is_sorted(reverse=True)

    assert_that(['a', 'x', 'x']).contains_duplicates()
    assert_that(['a', 'b', 'c']).does_not_contain_duplicates()

    assert_that(['a', 'b', 'c']).starts_with('a')
    assert_that(['a', 'b', 'c']).ends_with('c')


def test_tuples():
    assert_that(()).is_not_none()
    assert_that(()).is_empty()
    assert_that(()).is_false()
    assert_that(()).is_type_of(tuple)
    assert_that(()).is_instance_of(tuple)
    assert_that(()).is_iterable()

    assert_that((1, 2, 3)).is_length(3)
    assert_that((1, 2, 3)).is_not_empty()
    assert_that((1, 2, 3)).is_equal_to((1, 2, 3))
    assert_that((1, 2, 3)).is_not_equal_to((1, 2, 4))

    assert_that((1, 2, 3)).contains(1)
    assert_that((1, 2, 3)).contains(3, 2, 1)
    assert_that((1, 2, 3)).does_not_contain(4, 5, 6)
    assert_that((1, 2, 3)).contains_only(1, 2, 3)
    assert_that((1, 1, 1)).contains_only(1)
    assert_that((1, 2, 3)).contains_sequence(2, 3)
    assert_that((1, 2, 3)).is_subset_of((1, 2, 3, 4))
    assert_that((1, 2, 3)).is_sorted()
    assert_that((3, 2, 1)).is_sorted(reverse=True)

    assert_that((1, 2, 2)).contains_duplicates()
    assert_that((1, 2, 3)).does_not_contain_duplicates()

    assert_that((1, 2, 3)).starts_with(1)
    assert_that((1, 2, 3)).ends_with(3)


def test_dicts():
    assert_that({}).is_not_none()
    assert_that({}).is_empty()
    assert_that({}).is_false()
    assert_that({}).is_type_of(dict)
    assert_that({}).is_instance_of(dict)

    assert_that({'a': 1, 'b': 2}).is_length(2)
    assert_that({'a': 1, 'b': 2}).is_not_empty()
    assert_that({'a': 1, 'b': 2}).is_equal_to({'a': 1, 'b': 2})
    assert_that({'a': 1, 'b': 2}).is_equal_to({'b': 2, 'a': 1})
    assert_that({'a': 1, 'b': 2}).is_not_equal_to({'a': 1, 'b': 3})

    assert_that({'a': 1, 'b': 2}).contains('a')
    assert_that({'a': 1, 'b': 2}).contains('b', 'a')
    assert_that({'a': 1, 'b': 2}).does_not_contain('x')
    assert_that({'a': 1, 'b': 2}).does_not_contain('x', 'y')
    assert_that({'a': 1, 'b': 2}).contains_only('a', 'b')
    assert_that({'a': 1, 'b': 2}).is_subset_of({'a': 1, 'b': 2, 'c': 3})

    # contains_key() is just an alias for contains()
    assert_that({'a': 1, 'b': 2}).contains_key('a')
    assert_that({'a': 1, 'b': 2}).contains_key('b', 'a')

    # does_not_contain_key() is just an alias for does_not_contain()
    assert_that({'a': 1, 'b': 2}).does_not_contain_key('x')
    assert_that({'a': 1, 'b': 2}).does_not_contain_key('x', 'y')

    assert_that({'a': 1, 'b': 2}).contains_value(1)
    assert_that({'a': 1, 'b': 2}).contains_value(2, 1)
    assert_that({'a': 1, 'b': 2}).does_not_contain_value(3)
    assert_that({'a': 1, 'b': 2}).does_not_contain_value(3, 4)

    assert_that({'a': 1, 'b': 2}).contains_entry({'a': 1})
    assert_that({'a': 1, 'b': 2}).contains_entry({'a': 1}, {'b': 2})
    assert_that({'a': 1, 'b': 2}).does_not_contain_entry({'a': 2})
    assert_that({'a': 1, 'b': 2}).does_not_contain_entry({'a': 2}, {'b': 1})

    # lists of dicts can be flattened on key
    fred = {'first_name': 'Fred', 'last_name': 'Smith'}
    bob = {'first_name': 'Bob', 'last_name': 'Barr'}
    people = [fred, bob]

    assert_that(people).extracting('first_name').is_equal_to(['Fred', 'Bob'])
    assert_that(people).extracting('first_name').contains('Fred', 'Bob')


def test_dict_compare():
    # ignore
    assert_that({'a': 1, 'b': 2}).is_equal_to({'a': 1}, ignore='b')
    assert_that({'a': 1, 'b': 2, 'c': 3}).is_equal_to({'a': 1}, ignore=['b', 'c'])
    assert_that({'a': 1, 'b': {'c': 2, 'd': 3}}).is_equal_to({'a': 1, 'b': {'c': 2}}, ignore=('b', 'd'))

    # include
    assert_that({'a': 1, 'b': 2}).is_equal_to({'a': 1}, include='a')
    assert_that({'a': 1, 'b': 2, 'c': 3}).is_equal_to({'a': 1, 'b': 2}, include=['a', 'b'])
    assert_that({'a': 1, 'b': {'c': 2, 'd': 3}}).is_equal_to({'b': {'d': 3}}, include=('b', 'd'))

    # both
    assert_that({'a': 1, 'b': {'c': 2, 'd': 3, 'e': 4, 'f': 5}}).is_equal_to(
        {'b': {'d': 3, 'f': 5}},
        ignore=[('b', 'c'), ('b', 'e')],
        include='b'
    )


def test_sets():
    assert_that(set([])).is_not_none()
    assert_that(set([])).is_empty()
    assert_that(set([])).is_false()
    assert_that(set([])).is_type_of(set)
    assert_that(set([])).is_instance_of(set)

    assert_that(set(['a', 'b'])).is_length(2)
    assert_that(set(['a', 'b'])).is_not_empty()
    assert_that(set(['a', 'b'])).is_equal_to(set(['a', 'b']))
    assert_that(set(['a', 'b'])).is_equal_to(set(['b', 'a']))
    assert_that(set(['a', 'b'])).is_not_equal_to(set(['a', 'x']))

    assert_that(set(['a', 'b'])).contains('a')
    assert_that(set(['a', 'b'])).contains('b', 'a')
    assert_that(set(['a', 'b'])).does_not_contain('x', 'y')
    assert_that(set(['a', 'b'])).contains_only('a', 'b')
    assert_that(set(['a', 'b'])).is_subset_of(set(['a', 'b', 'c']))
    assert_that(set(['a', 'b'])).is_subset_of(set(['a']), set(['b']))


def test_booleans():
    assert_that(True).is_true()
    assert_that(False).is_false()
    assert_that(True).is_type_of(bool)


def test_dates():
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)

    assert_that(yesterday).is_before(today)
    assert_that(today).is_after(yesterday)

    today_0us = today - datetime.timedelta(microseconds=today.microsecond)
    today_0s = today - datetime.timedelta(seconds=today.second)
    today_0h = today - datetime.timedelta(hours=today.hour)

    assert_that(today).is_equal_to_ignoring_milliseconds(today_0us)
    assert_that(today).is_equal_to_ignoring_seconds(today_0s)
    assert_that(today).is_equal_to_ignoring_time(today_0h)
    assert_that(today).is_equal_to(today)

    middle = today - datetime.timedelta(hours=12)
    hours_24 = datetime.timedelta(hours=24)

    assert_that(today).is_greater_than(yesterday)
    assert_that(yesterday).is_less_than(today)
    assert_that(middle).is_between(yesterday, today)

    # note that the tolerance must be a datetime.timedelta object
    assert_that(yesterday).is_close_to(today, hours_24)

    # 1980-01-02 03:04:05.000006
    x = datetime.datetime(1980, 1, 2, 3, 4, 5, 6)

    assert_that(x).has_year(1980)
    assert_that(x).has_month(1)
    assert_that(x).has_day(2)
    assert_that(x).has_hour(3)
    assert_that(x).has_minute(4)
    assert_that(x).has_second(5)
    assert_that(x).has_microsecond(6)


def test_files():
    # setup
    with open('foo.txt', 'w') as fp:
        fp.write('foobar')

    assert_that('foo.txt').exists()
    assert_that('missing.txt').does_not_exist()
    assert_that('foo.txt').is_file()

    # assert_that('mydir').exists()
    assert_that('missing_dir').does_not_exist()
    # assert_that('mydir').is_directory()

    assert_that('foo.txt').is_named('foo.txt')
    # assert_that('foo.txt').is_child_of('mydir')

    contents = contents_of('foo.txt', 'ascii')
    assert_that(contents).starts_with('foo').ends_with('bar').contains('oob')

    # teardown
    os.remove('foo.txt')


def test_objects():
    fred = Person('Fred', 'Smith')

    assert_that(fred).is_not_none()
    assert_that(fred).is_true()
    assert_that(fred).is_type_of(Person)
    assert_that(fred).is_instance_of(object)
    assert_that(fred).is_same_as(fred)

    assert_that(fred.first_name).is_equal_to('Fred')
    assert_that(fred.name).is_equal_to('Fred Smith')
    assert_that(fred.say_hello()).is_equal_to('Hello, Fred!')

    fred = Person('Fred', 'Smith')
    bob = Person('Bob', 'Barr')
    people = [fred, bob]

    assert_that(people).extracting('first_name').is_equal_to(['Fred', 'Bob'])
    assert_that(people).extracting('first_name').contains('Fred', 'Bob')
    assert_that(people).extracting('first_name').does_not_contain('Charlie')

    fred = Person('Fred', 'Smith')
    joe = Developer('Joe', 'Coder')
    people = [fred, joe]

    assert_that(people).extracting('first_name').contains('Fred', 'Joe')

    assert_that(people).extracting('first_name', 'last_name').contains(('Fred', 'Smith'), ('Joe', 'Coder'))

    assert_that(people).extracting('name').contains('Fred Smith', 'Joe Coder')
    assert_that(people).extracting('say_hello').contains('Hello, Fred!', 'Joe writes code.')


def test_dyn():
    fred = Person('Fred', 'Smith')

    assert_that(fred.first_name).is_equal_to('Fred')
    assert_that(fred.name).is_equal_to('Fred Smith')
    assert_that(fred.say_hello()).is_equal_to('Hello, Fred!')

    assert_that(fred).has_first_name('Fred')
    assert_that(fred).has_name('Fred Smith')
    assert_that(fred).has_say_hello('Hello, Fred!')


def test_failure():
    try:
        some_func('foo')
        fail('should have raised error')
    except RuntimeError as e:
        assert_that(str(e)).is_equal_to('some err')


def test_expected_exceptions():
    assert_that(some_func).raises(RuntimeError).when_called_with('foo')
    assert_that(some_func).raises(RuntimeError).when_called_with('foo')\
        .is_length(8).starts_with('some').is_equal_to('some err')


def test_custom_error_message():
    try:
        assert_that(1+2).is_equal_to(2)
        fail('should have raised error')
    except AssertionError as e:
        assert_that(str(e)).is_equal_to('Expected <3> to be equal to <2>, but was not.')

    try:
        assert_that(1+2).described_as('adding stuff').is_equal_to(2)
        fail('should have raised error')
    except AssertionError as e:
        assert_that(str(e)).is_equal_to('[adding stuff] Expected <3> to be equal to <2>, but was not.')


def test_assert_warn():
    assert_warn('foo').is_length(4)
    assert_warn('foo').is_empty()
    assert_warn('foo').is_false()
    assert_warn('foo').is_digit()
    assert_warn('123').is_alpha()
    assert_warn('foo').is_upper()
    assert_warn('FOO').is_lower()
    assert_warn('foo').is_equal_to('bar')
    assert_warn('foo').is_not_equal_to('foo')
    assert_warn('foo').is_equal_to_ignoring_case('BAR')


def test_soft_assertions():
    try:
        with soft_assertions():
            assert_that('foo').is_length(4)
            assert_that('foo').is_empty()
            assert_that('foo').is_false()
            assert_that('foo').is_digit()
            assert_that('123').is_alpha()
            assert_that('foo').is_upper()
            assert_that('FOO').is_lower()
            assert_that('foo').is_equal_to('bar')
            assert_that('foo').is_not_equal_to('foo')
            assert_that('foo').is_equal_to_ignoring_case('BAR')
        fail('should have raised error')
    except AssertionError as e:
        assert_that(str(e)).contains('1. Expected <foo> to be of length <4>, but was <3>.')
        assert_that(str(e)).contains('2. Expected <foo> to be empty string, but was not.')
        assert_that(str(e)).contains('3. Expected <False>, but was not.')
        assert_that(str(e)).contains('4. Expected <foo> to contain only digits, but did not.')
        assert_that(str(e)).contains('5. Expected <123> to contain only alphabetic chars, but did not.')
        assert_that(str(e)).contains('6. Expected <foo> to contain only uppercase chars, but did not.')
        assert_that(str(e)).contains('7. Expected <FOO> to contain only lowercase chars, but did not.')
        assert_that(str(e)).contains('8. Expected <foo> to be equal to <bar>, but was not.')
        assert_that(str(e)).contains('9. Expected <foo> to be not equal to <foo>, but was.')
        assert_that(str(e)).contains('10. Expected <foo> to be case-insensitive equal to <BAR>, but was not.')


def test_chaining():
    fred = Person('Fred', 'Smith')
    joe = Person('Joe', 'Jones')
    people = [fred, joe]

    assert_that('foo').is_length(3).starts_with('f').ends_with('oo')

    assert_that([1, 2, 3]).is_type_of(list).contains(1, 2).does_not_contain(4, 5)

    assert_that(fred).has_first_name('Fred').has_last_name('Smith').has_shoe_size(12)

    assert_that(people).is_length(2).extracting('first_name').contains('Fred', 'Joe')


def some_func(arg):
    raise RuntimeError('some err')


class Person(object):
    def __init__(self, first_name, last_name, shoe_size=12):
        self.first_name = first_name
        self.last_name = last_name
        self.shoe_size = shoe_size

    @property
    def name(self):
        return '%s %s' % (self.first_name, self.last_name)

    def say_hello(self):
        return 'Hello, %s!' % self.first_name


class Developer(Person):
    def say_hello(self):
        return '%s writes code.' % self.first_name
