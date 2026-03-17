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
import numbers
import datetime
import collections

if sys.version_info[0] == 3:
    Iterable = collections.abc.Iterable
else:
    Iterable = collections.Iterable

__tracebackhide__ = True


class HelpersMixin(object):
    """Helpers mixin.  For internal use only."""

    def _fmt_items(self, i):
        """Helper to format the given items."""
        if len(i) == 0:
            return '<>'
        elif len(i) == 1 and hasattr(i, '__getitem__'):
            return '<%s>' % i[0]
        else:
            return '<%s>' % str(i).lstrip('([').rstrip(',])')

    def _fmt_args_kwargs(self, *some_args, **some_kwargs):
        """Helper to convert the given args and kwargs into a string."""
        if some_args:
            out_args = str(some_args).lstrip('(').rstrip(',)')
        if some_kwargs:
            out_kwargs = ', '.join([str(i).lstrip('(').rstrip(')').replace(', ', ': ') for i in [
                    (k, some_kwargs[k]) for k in sorted(some_kwargs.keys())]])

        if some_args and some_kwargs:
            return out_args + ', ' + out_kwargs
        elif some_args:
            return out_args
        elif some_kwargs:
            return out_kwargs
        else:
            return ''

    def _validate_between_args(self, val_type, low, high):
        """Helper to validate given range args."""
        low_type = type(low)
        high_type = type(high)

        if val_type in self._NUMERIC_NON_COMPAREABLE:
            raise TypeError('ordering is not defined for type <%s>' % val_type.__name__)

        if val_type in self._NUMERIC_COMPAREABLE:
            if low_type is not val_type:
                raise TypeError('given low arg must be <%s>, but was <%s>' % (val_type.__name__, low_type.__name__))
            if high_type is not val_type:
                raise TypeError('given high arg must be <%s>, but was <%s>' % (val_type.__name__, low_type.__name__))
        elif isinstance(self.val, numbers.Number):
            if isinstance(low, numbers.Number) is False:
                raise TypeError('given low arg must be numeric, but was <%s>' % low_type.__name__)
            if isinstance(high, numbers.Number) is False:
                raise TypeError('given high arg must be numeric, but was <%s>' % high_type.__name__)
        else:
            raise TypeError('ordering is not defined for type <%s>' % val_type.__name__)

        if low > high:
            raise ValueError('given low arg must be less than given high arg')

    def _validate_close_to_args(self, val, other, tolerance):
        """Helper for validate given arg and delta."""
        if type(val) is complex or type(other) is complex or type(tolerance) is complex:
            raise TypeError('ordering is not defined for complex numbers')

        if isinstance(val, numbers.Number) is False and type(val) is not datetime.datetime:
            raise TypeError('val is not numeric or datetime')

        if type(val) is datetime.datetime:
            if type(other) is not datetime.datetime:
                raise TypeError('given arg must be datetime, but was <%s>' % type(other).__name__)
            if type(tolerance) is not datetime.timedelta:
                raise TypeError('given tolerance arg must be timedelta, but was <%s>' % type(tolerance).__name__)
        else:
            if isinstance(other, numbers.Number) is False:
                raise TypeError('given arg must be numeric')
            if isinstance(tolerance, numbers.Number) is False:
                raise TypeError('given tolerance arg must be numeric')
            if tolerance < 0:
                raise ValueError('given tolerance arg must be positive')

    def _check_dict_like(self, d, check_keys=True, check_values=True, check_getitem=True, name='val', return_as_bool=False):
        """Helper to check if given val has various dict-like attributes."""
        if not isinstance(d, Iterable):
            if return_as_bool:
                return False
            else:
                raise TypeError('%s <%s> is not dict-like: not iterable' % (name, type(d).__name__))
        if check_keys:
            if not hasattr(d, 'keys') or not callable(getattr(d, 'keys')):
                if return_as_bool:
                    return False
                else:
                    raise TypeError('%s <%s> is not dict-like: missing keys()' % (name, type(d).__name__))
        if check_values:
            if not hasattr(d, 'values') or not callable(getattr(d, 'values')):
                if return_as_bool:
                    return False
                else:
                    raise TypeError('%s <%s> is not dict-like: missing values()' % (name, type(d).__name__))
        if check_getitem:
            if not hasattr(d, '__getitem__'):
                if return_as_bool:
                    return False
                else:
                    raise TypeError('%s <%s> is not dict-like: missing [] accessor' % (name, type(d).__name__))
        if return_as_bool:
            return True

    def _check_iterable(self, l, check_getitem=True, name='val'):
        """Helper to check if given val has various iterable attributes."""
        if not isinstance(l, Iterable):
            raise TypeError('%s <%s> is not iterable' % (name, type(l).__name__))
        if check_getitem:
            if not hasattr(l, '__getitem__'):
                raise TypeError('%s <%s> does not have [] accessor' % (name, type(l).__name__))

    def _dict_not_equal(self, val, other, ignore=None, include=None):
        """Helper to compare dicts."""
        if ignore or include:
            ignores = self._dict_ignore(ignore)
            includes = self._dict_include(include)

            # guarantee include keys are in val
            if include:
                missing = []
                for i in includes:
                    if i not in val:
                        missing.append(i)
                if missing:
                    self.error('Expected <%s> to include key%s %s, but did not include key%s %s.' % (
                        val,
                        '' if len(includes) == 1 else 's',
                        self._fmt_items(['.'.join([str(s) for s in i]) if type(i) is tuple else i for i in includes]),
                        '' if len(missing) == 1 else 's',
                        self._fmt_items(missing)))

            # calc val keys given ignores and includes
            if ignore and include:
                k1 = set([k for k in val if k not in ignores and k in includes])
            elif ignore:
                k1 = set([k for k in val if k not in ignores])
            else:  # include
                k1 = set([k for k in val if k in includes])

            # calc other keys given ignores and includes
            if ignore and include:
                k2 = set([k for k in other if k not in ignores and k in includes])
            elif ignore:
                k2 = set([k for k in other if k not in ignores])
            else:  # include
                k2 = set([k for k in other if k in includes])

            if k1 != k2:
                # different set of keys, so not equal
                return True
            else:
                for k in k1:
                    if self._check_dict_like(val[k], check_values=False, return_as_bool=True) and \
                            self._check_dict_like(other[k], check_values=False, return_as_bool=True):
                        subdicts_not_equal = self._dict_not_equal(
                            val[k],
                            other[k],
                            ignore=[i[1:] for i in ignores if type(i) is tuple and i[0] == k] if ignore else None,
                            include=[i[1:] for i in self._dict_ignore(include) if type(i) is tuple and i[0] == k] if include else None)
                        if subdicts_not_equal:
                            # fast fail inside the loop since sub-dicts are not equal
                            return True
                    elif val[k] != other[k]:
                        # fast fail inside the loop since values are not equal
                        return True
            return False
        else:
            return val != other

    def _dict_ignore(self, ignore):
        """Helper to make list for given ignore kwarg values."""
        return [i[0] if type(i) is tuple and len(i) == 1 else i for i in (ignore if type(ignore) is list else [ignore])]

    def _dict_include(self, include):
        """Helper to make a list from given include kwarg values."""
        return [i[0] if type(i) is tuple else i for i in (include if type(include) is list else [include])]

    def _dict_err(self, val, other, ignore=None, include=None):
        """Helper to construct error message for dict comparison."""
        def _dict_repr(d, other):
            out = ''
            ellip = False
            for k, v in sorted(d.items()):
                if k not in other:
                    out += '%s%s: %s' % (', ' if len(out) > 0 else '', repr(k), repr(v))
                elif v != other[k]:
                    out += '%s%s: %s' % (
                        ', ' if len(out) > 0 else '',
                        repr(k),
                        _dict_repr(v, other[k]) if self._check_dict_like(
                            v, check_values=False, return_as_bool=True) and self._check_dict_like(
                                other[k], check_values=False, return_as_bool=True) else repr(v)
                    )
                else:
                    ellip = True
            return '{%s%s}' % ('..' if ellip and len(out) == 0 else '.., ' if ellip else '', out)

        if ignore:
            ignores = self._dict_ignore(ignore)
            ignore_err = ' ignoring keys %s' % self._fmt_items(['.'.join([str(s) for s in i]) if type(i) is tuple else i for i in ignores])
        if include:
            includes = self._dict_ignore(include)
            include_err = ' including keys %s' % self._fmt_items(['.'.join([str(s) for s in i]) if type(i) is tuple else i for i in includes])

        self.error('Expected <%s> to be equal to <%s>%s%s, but was not.' % (
            _dict_repr(val, other),
            _dict_repr(other, val),
            ignore_err if ignore else '',
            include_err if include else ''
        ))
