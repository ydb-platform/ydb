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

import datetime

__tracebackhide__ = True


class DateMixin(object):
    """Date and time assertions mixin."""

    def is_before(self, other):
        """Asserts that val is a date and is before other date.

        Args:
            other: the other date, expected to be after val

        Examples:
            Usage::

                import datetime

                today = datetime.datetime.now()
                yesterday = today - datetime.timedelta(days=1)

                assert_that(yesterday).is_before(today)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** before the given date
        """
        if type(self.val) is not datetime.datetime:
            raise TypeError('val must be datetime, but was type <%s>' % type(self.val).__name__)
        if type(other) is not datetime.datetime:
            raise TypeError('given arg must be datetime, but was type <%s>' % type(other).__name__)
        if self.val >= other:
            self.error('Expected <%s> to be before <%s>, but was not.' % (self.val.strftime('%Y-%m-%d %H:%M:%S'), other.strftime('%Y-%m-%d %H:%M:%S')))
        return self

    def is_after(self, other):
        """Asserts that val is a date and is after other date.

        Args:
            other: the other date, expected to be before val

        Examples:
            Usage::

                import datetime

                today = datetime.datetime.now()
                yesterday = today - datetime.timedelta(days=1)

                assert_that(today).is_after(yesterday)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** after the given date
        """
        if type(self.val) is not datetime.datetime:
            raise TypeError('val must be datetime, but was type <%s>' % type(self.val).__name__)
        if type(other) is not datetime.datetime:
            raise TypeError('given arg must be datetime, but was type <%s>' % type(other).__name__)
        if self.val <= other:
            self.error('Expected <%s> to be after <%s>, but was not.' % (self.val.strftime('%Y-%m-%d %H:%M:%S'), other.strftime('%Y-%m-%d %H:%M:%S')))
        return self

    def is_equal_to_ignoring_milliseconds(self, other):
        """Asserts that val is a date and is equal to other date to the second.

        Args:
            other: the other date, expected to be equal to the second

        Examples:
            Usage::

                import datetime

                d1 = datetime.datetime(2020, 1, 2, 3, 4, 5, 6)       # 2020-01-02 03:04:05.000006
                d2 = datetime.datetime(2020, 1, 2, 3, 4, 5, 777777)  # 2020-01-02 03:04:05.777777

                assert_that(d1).is_equal_to_ignoring_milliseconds(d2)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** equal to the given date to the second
        """
        if type(self.val) is not datetime.datetime:
            raise TypeError('val must be datetime, but was type <%s>' % type(self.val).__name__)
        if type(other) is not datetime.datetime:
            raise TypeError('given arg must be datetime, but was type <%s>' % type(other).__name__)
        if self.val.date() != other.date() or self.val.hour != other.hour or self.val.minute != other.minute or self.val.second != other.second:
            self.error('Expected <%s> to be equal to <%s>, but was not.' % (self.val.strftime('%Y-%m-%d %H:%M:%S'), other.strftime('%Y-%m-%d %H:%M:%S')))
        return self

    def is_equal_to_ignoring_seconds(self, other):
        """Asserts that val is a date and is equal to other date to the minute.

        Args:
            other: the other date, expected to be equal to the minute

        Examples:
            Usage::

                import datetime

                d1 = datetime.datetime(2020, 1, 2, 3, 4, 5)   # 2020-01-02 03:04:05
                d2 = datetime.datetime(2020, 1, 2, 3, 4, 55)  # 2020-01-02 03:04:55

                assert_that(d1).is_equal_to_ignoring_seconds(d2)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** equal to the given date to the minute
        """
        if type(self.val) is not datetime.datetime:
            raise TypeError('val must be datetime, but was type <%s>' % type(self.val).__name__)
        if type(other) is not datetime.datetime:
            raise TypeError('given arg must be datetime, but was type <%s>' % type(other).__name__)
        if self.val.date() != other.date() or self.val.hour != other.hour or self.val.minute != other.minute:
            self.error('Expected <%s> to be equal to <%s>, but was not.' % (self.val.strftime('%Y-%m-%d %H:%M'), other.strftime('%Y-%m-%d %H:%M')))
        return self

    def is_equal_to_ignoring_time(self, other):
        """Asserts that val is a date and is equal to other date ignoring time.

        Args:
            other: the other date, expected to be equal ignoring time

        Examples:
            Usage::

                import datetime

                d1 = datetime.datetime(2020, 1, 2, 3, 4, 5)     # 2020-01-02 03:04:05
                d2 = datetime.datetime(2020, 1, 2, 13, 44, 55)  # 2020-01-02 13:44:55

                assert_that(d1).is_equal_to_ignoring_time(d2)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** equal to the given date ignoring time
        """
        if type(self.val) is not datetime.datetime:
            raise TypeError('val must be datetime, but was type <%s>' % type(self.val).__name__)
        if type(other) is not datetime.datetime:
            raise TypeError('given arg must be datetime, but was type <%s>' % type(other).__name__)
        if self.val.date() != other.date():
            self.error('Expected <%s> to be equal to <%s>, but was not.' % (self.val.strftime('%Y-%m-%d'), other.strftime('%Y-%m-%d')))
        return self
