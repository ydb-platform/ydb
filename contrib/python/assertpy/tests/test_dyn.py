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

from assertpy import assert_that, fail


class Person(object):
    def __init__(self, first_name, last_name, shoe_size):
        self.first_name = first_name
        self.last_name = last_name
        self.shoe_size = shoe_size

    @property
    def name(self):
        return '%s %s' % (self.first_name, self.last_name)

    def say_hello(self):
        return 'Hello, %s!' % self.first_name

    def say_goodbye(self, target):
        return 'Bye, %s!' % target


fred = Person('Fred', 'Smith', 12)


def test_dynamic_assertion():
    assert_that(fred).is_type_of(Person)
    assert_that(fred).is_instance_of(object)

    assert_that(fred.first_name).is_equal_to('Fred')
    assert_that(fred.last_name).is_equal_to('Smith')
    assert_that(fred.shoe_size).is_equal_to(12)

    assert_that(fred).has_first_name('Fred')
    assert_that(fred).has_last_name('Smith')
    assert_that(fred).has_shoe_size(12)


def test_dynamic_assertion_on_property():
    assert_that(fred.name).is_equal_to('Fred Smith')
    assert_that(fred).has_name('Fred Smith')


def test_dynamic_assertion_on_method():
    assert_that(fred.say_hello()).is_equal_to('Hello, Fred!')
    assert_that(fred).has_say_hello('Hello, Fred!')


def test_dynamic_assertion_failure():
    try:
        assert_that(fred).has_first_name('Joe')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <Fred> to be equal to <Joe> on attribute <first_name>, but was not.')


def test_dynamic_assertion_bad_name_failure():
    try:
        assert_that(fred).foo()
        fail('should have raised error')
    except AttributeError as ex:
        assert_that(str(ex)).is_equal_to('assertpy has no assertion <foo()>')


def test_dynamic_assertion_unknown_attribute_failure():
    try:
        assert_that(fred).has_foo()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected attribute <foo>, but val has no attribute <foo>.')


def test_dynamic_assertion_no_args_failure():
    try:
        assert_that(fred).has_first_name()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('assertion <has_first_name()> takes exactly 1 argument (0 given)')


def test_dynamic_assertion_too_many_args_failure():
    try:
        assert_that(fred).has_first_name('Fred', 'Joe')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('assertion <has_first_name()> takes exactly 1 argument (2 given)')


def test_dynamic_assertion_on_method_failure():
    try:
        assert_that(fred).has_say_goodbye('Foo')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('val does not have zero-arg method <say_goodbye()>')


def test_chaining():
    assert_that(fred).has_first_name('Fred').has_last_name('Smith').has_shoe_size(12)
