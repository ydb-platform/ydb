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
import collections

if sys.version_info[0] == 3:
    Iterable = collections.abc.Iterable
else:
    Iterable = collections.Iterable

__tracebackhide__ = True


class DynamicMixin(object):
    """Dynamic assertions mixin.

    When testing attributes of an object (or the contents of a dict), the
    :meth:`~assertpy.base.BaseMixin.is_equal_to` assertion can be a bit verbose::

        fred = Person('Fred', 'Smith')

        assert_that(fred.first_name).is_equal_to('Fred')
        assert_that(fred.name).is_equal_to('Fred Smith')
        assert_that(fred.say_hello()).is_equal_to('Hello, Fred!')

    Instead, use dynamic assertions in the form of ``has_<name>()`` where ``<name>`` is the name of
    any attribute, property, or zero-argument method on the given object. Dynamic equality
    assertions test if actual is equal to expected using the ``==`` operator. Using dynamic
    assertions, we can rewrite the above example as::

        assert_that(fred).has_first_name('Fred')
        assert_that(fred).has_name('Fred Smith')
        assert_that(fred).has_say_hello('Hello, Fred!')

    Similarly, dynamic assertions also work on any *dict-like* object::

        fred = {
            'first_name': 'Fred',
            'last_name': 'Smith',
            'shoe_size': 12
        }

        assert_that(fred).has_first_name('Fred')
        assert_that(fred).has_last_name('Smith')
        assert_that(fred).has_shoe_size(12)
    """

    def __getattr__(self, attr):
        """Asserts that val has attribute attr and that its value is equal to other via a dynamic
        assertion of the form ``has_<attr>()``."""
        if not attr.startswith('has_'):
            raise AttributeError('assertpy has no assertion <%s()>' % attr)

        attr_name = attr[4:]
        err_msg = False
        is_dict = isinstance(self.val, Iterable) and hasattr(self.val, '__getitem__')

        if not hasattr(self.val, attr_name):
            if is_dict:
                if attr_name not in self.val:
                    err_msg = 'Expected key <%s>, but val has no key <%s>.' % (attr_name, attr_name)
            else:
                err_msg = 'Expected attribute <%s>, but val has no attribute <%s>.' % (attr_name, attr_name)

        def _wrapper(*args, **kwargs):
            if err_msg:
                self.error(err_msg)  # ok to raise AssertionError now that we are inside wrapper
            else:
                if len(args) != 1:
                    raise TypeError('assertion <%s()> takes exactly 1 argument (%d given)' % (attr, len(args)))

                if is_dict:
                    val_attr = self.val[attr_name]
                else:
                    val_attr = getattr(self.val, attr_name)

                if callable(val_attr):
                    try:
                        actual = val_attr()
                    except TypeError:
                        raise TypeError('val does not have zero-arg method <%s()>' % attr_name)
                else:
                    actual = val_attr

                expected = args[0]
                if actual != expected:
                    self.error('Expected <%s> to be equal to <%s> on %s <%s>, but was not.' % (actual, expected, 'key' if is_dict else 'attribute', attr_name))
            return self

        return _wrapper
