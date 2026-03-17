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

__tracebackhide__ = True


class BaseMixin(object):
    """Base mixin."""

    def described_as(self, description):
        """Describes the assertion.  On failure, the description is included in the error message.

        This is not an assertion itself.  But if the any of the following chained assertions fail,
        the description will be included in addition to the regular error message.

        Args:
            description: the error message description

        Examples:
            Usage::

                assert_that(1).described_as('error msg desc').is_equal_to(2)  # fails
                # [error msg desc] Expected <1> to be equal to <2>, but was not.

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion
        """
        self.description = str(description)
        return self

    def is_equal_to(self, other, **kwargs):
        """Asserts that val is equal to other.

        Checks actual is equal to expected using the ``==`` operator. When val is *dict-like*,
        optionally ignore or include keys when checking equality.

        Args:
            other: the expected value
            **kwargs: see below

        Keyword Args:
            ignore: the dict key (or list of keys) to ignore
            include: the dict key (of list of keys) to include

        Examples:
            Usage::

                assert_that(1 + 2).is_equal_to(3)
                assert_that('foo').is_equal_to('foo')
                assert_that(123).is_equal_to(123)
                assert_that(123.4).is_equal_to(123.4)
                assert_that(['a', 'b']).is_equal_to(['a', 'b'])
                assert_that((1, 2, 3)).is_equal_to((1, 2, 3))
                assert_that({'a': 1, 'b': 2}).is_equal_to({'a': 1, 'b': 2})
                assert_that({'a', 'b'}).is_equal_to({'a', 'b'})

            When the val is *dict-like*, keys can optionally be *ignored* when checking equality::

                # ignore a single key
                assert_that({'a': 1, 'b': 2}).is_equal_to({'a': 1}, ignore='b')

                # ignore multiple keys
                assert_that({'a': 1, 'b': 2, 'c': 3}).is_equal_to({'a': 1}, ignore=['b', 'c'])

                # ignore nested keys
                assert_that({'a': {'b': 2, 'c': 3, 'd': 4}}).is_equal_to({'a': {'d': 4}}, ignore=[('a', 'b'), ('a', 'c')])

            When the val is *dict-like*, only certain keys can be *included* when checking equality::

                # include a single key
                assert_that({'a': 1, 'b': 2}).is_equal_to({'a': 1}, include='a')

                # include multiple keys
                assert_that({'a': 1, 'b': 2, 'c': 3}).is_equal_to({'a': 1, 'b': 2}, include=['a', 'b'])

            Failure produces a nice error message::

                assert_that(1).is_equal_to(2)  # fails
                # Expected <1> to be equal to <2>, but was not.

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if actual is **not** equal to expected

        Tip:
            Using :meth:`is_equal_to` with a ``float`` val is just asking for trouble. Instead, you'll
            always want to use *fuzzy* numeric assertions like :meth:`~assertpy.numeric.NumericMixin.is_close_to`
            or :meth:`~assertpy.numeric.NumericMixin.is_between`.

        See Also:
            :meth:`~assertpy.string.StringMixin.is_equal_to_ignoring_case` - for case-insensitive string equality
        """
        if self._check_dict_like(self.val, check_values=False, return_as_bool=True) and \
                self._check_dict_like(other, check_values=False, return_as_bool=True):
            if self._dict_not_equal(self.val, other, ignore=kwargs.get('ignore'), include=kwargs.get('include')):
                self._dict_err(self.val, other, ignore=kwargs.get('ignore'), include=kwargs.get('include'))
        else:
            if self.val != other:
                self.error('Expected <%s> to be equal to <%s>, but was not.' % (self.val, other))
        return self

    def is_not_equal_to(self, other):
        """Asserts that val is not equal to other.

        Checks actual is not equal to expected using the ``!=`` operator.

        Args:
            other: the expected value

        Examples:
            Usage::

                assert_that(1 + 2).is_not_equal_to(4)
                assert_that('foo').is_not_equal_to('bar')
                assert_that(123).is_not_equal_to(456)
                assert_that(123.4).is_not_equal_to(567.8)
                assert_that(['a', 'b']).is_not_equal_to(['c', 'd'])
                assert_that((1, 2, 3)).is_not_equal_to((1, 2, 4))
                assert_that({'a': 1, 'b': 2}).is_not_equal_to({'a': 1, 'b': 3})
                assert_that({'a', 'b'}).is_not_equal_to({'a', 'x'})

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if actual **is** equal to expected
        """
        if self.val == other:
            self.error('Expected <%s> to be not equal to <%s>, but was.' % (self.val, other))
        return self

    def is_same_as(self, other):
        """Asserts that val is identical to other.

        Checks actual is identical to expected using the ``is`` operator.

        Args:
            other: the expected value

        Examples:
            Basic types are identical::

                assert_that(1).is_same_as(1)
                assert_that('foo').is_same_as('foo')
                assert_that(123.4).is_same_as(123.4)

            As are immutables like ``tuple``::

                assert_that((1, 2, 3)).is_same_as((1, 2, 3))

            But mutable collections like ``list``, ``dict``, and ``set`` are not::

                # these all fail...
                assert_that(['a', 'b']).is_same_as(['a', 'b'])  # fails
                assert_that({'a': 1, 'b': 2}).is_same_as({'a': 1, 'b': 2})  # fails
                assert_that({'a', 'b'}).is_same_as({'a', 'b'})  # fails

            Unless they are the same object::

                x = {'a': 1, 'b': 2}
                y = x
                assert_that(x).is_same_as(y)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if actual is **not** identical to expected
        """
        if self.val is not other:
            self.error('Expected <%s> to be identical to <%s>, but was not.' % (self.val, other))
        return self

    def is_not_same_as(self, other):
        """Asserts that val is not identical to other.

        Checks actual is not identical to expected using the ``is`` operator.

        Args:
            other: the expected value

        Examples:
            Usage::

                assert_that(1).is_not_same_as(2)
                assert_that('foo').is_not_same_as('bar')
                assert_that(123.4).is_not_same_as(567.8)
                assert_that((1, 2, 3)).is_not_same_as((1, 2, 4))

                # mutable collections, even if equal, are not identical...
                assert_that(['a', 'b']).is_not_same_as(['a', 'b'])
                assert_that({'a': 1, 'b': 2}).is_not_same_as({'a': 1, 'b': 2})
                assert_that({'a', 'b'}).is_not_same_as({'a', 'b'})

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if actual **is** identical to expected
        """
        if self.val is other:
            self.error('Expected <%s> to be not identical to <%s>, but was.' % (self.val, other))
        return self

    def is_true(self):
        """Asserts that val is true.

        Examples:
            Usage::

                assert_that(True).is_true()
                assert_that(1).is_true()
                assert_that('foo').is_true()
                assert_that(1.0).is_true()
                assert_that(['a', 'b']).is_true()
                assert_that((1, 2, 3)).is_true()
                assert_that({'a': 1, 'b': 2}).is_true()
                assert_that({'a', 'b'}).is_true()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **is** false
        """
        if not self.val:
            self.error('Expected <True>, but was not.')
        return self

    def is_false(self):
        """Asserts that val is false.

        Examples:
            Usage::

                assert_that(False).is_false()
                assert_that(0).is_false()
                assert_that('').is_false()
                assert_that(0.0).is_false()
                assert_that([]).is_false()
                assert_that(()).is_false()
                assert_that({}).is_false()
                assert_that(set()).is_false()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **is** true
        """
        if self.val:
            self.error('Expected <False>, but was not.')
        return self

    def is_none(self):
        """Asserts that val is none.

        Examples:
            Usage::

                assert_that(None).is_none()
                assert_that(print('hello world')).is_none()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** none
        """
        if self.val is not None:
            self.error('Expected <%s> to be <None>, but was not.' % self.val)
        return self

    def is_not_none(self):
        """Asserts that val is not none.

        Examples:
            Usage::

                assert_that(0).is_not_none()
                assert_that('foo').is_not_none()
                assert_that(False).is_not_none()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **is** none
        """
        if self.val is None:
            self.error('Expected not <None>, but was.')
        return self

    def _type(self, val):
        if hasattr(val, '__name__'):
            return val.__name__
        elif hasattr(val, '__class__'):
            return val.__class__.__name__
        return 'unknown'

    def is_type_of(self, some_type):
        """Asserts that val is of the given type.

        Args:
            some_type (type): the expected type

        Examples:
            Usage::

                assert_that(1).is_type_of(int)
                assert_that('foo').is_type_of(str)
                assert_that(123.4).is_type_of(float)
                assert_that(['a', 'b']).is_type_of(list)
                assert_that((1, 2, 3)).is_type_of(tuple)
                assert_that({'a': 1, 'b': 2}).is_type_of(dict)
                assert_that({'a', 'b'}).is_type_of(set)
                assert_that(True).is_type_of(bool)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** of the given type
        """
        if type(some_type) is not type and not issubclass(type(some_type), type):
            raise TypeError('given arg must be a type')
        if type(self.val) is not some_type:
            t = self._type(self.val)
            self.error('Expected <%s:%s> to be of type <%s>, but was not.' % (self.val, t, some_type.__name__))
        return self

    def is_instance_of(self, some_class):
        """Asserts that val is an instance of the given class.

        Args:
            some_class: the expected class

        Examples:
            Usage::

                assert_that(1).is_instance_of(int)
                assert_that('foo').is_instance_of(str)
                assert_that(123.4).is_instance_of(float)
                assert_that(['a', 'b']).is_instance_of(list)
                assert_that((1, 2, 3)).is_instance_of(tuple)
                assert_that({'a': 1, 'b': 2}).is_instance_of(dict)
                assert_that({'a', 'b'}).is_instance_of(set)
                assert_that(True).is_instance_of(bool)

            With a user-defined class::

                class Foo: pass
                f = Foo()
                assert_that(f).is_instance_of(Foo)
                assert_that(f).is_instance_of(object)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** an instance of the given class
        """
        try:
            if not isinstance(self.val, some_class):
                t = self._type(self.val)
                self.error('Expected <%s:%s> to be instance of class <%s>, but was not.' % (self.val, t, some_class.__name__))
        except TypeError:
            raise TypeError('given arg must be a class')
        return self

    def is_length(self, length):
        """Asserts that val is the given length.

        Checks val is the given length using the ``len()`` built-in.

        Args:
            length (int): the expected length

        Examples:
            Usage::

                assert_that('foo').is_length(3)
                assert_that(['a', 'b']).is_length(2)
                assert_that((1, 2, 3)).is_length(3)
                assert_that({'a': 1, 'b': 2}).is_length(2)
                assert_that({'a', 'b'}).is_length(2)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** the given length
        """
        if type(length) is not int:
            raise TypeError('given arg must be an int')
        if length < 0:
            raise ValueError('given arg must be a positive int')
        if len(self.val) != length:
            self.error('Expected <%s> to be of length <%d>, but was <%d>.' % (self.val, length, len(self.val)))
        return self
