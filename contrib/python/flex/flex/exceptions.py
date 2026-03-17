from __future__ import unicode_literals

import six
import collections

from flex._compat import Mapping
from flex.utils import (
    is_non_string_iterable,
    prettify_errors,
)


class ErrorCollectionMixin(object):
    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        if any((type_, value, traceback)):
            if not issubclass(type_, ValidationError):
                return False
        if self:
            self.raise_()

    def raise_(self):
        raise ValidationError(self)


class ErrorList(ErrorCollectionMixin, list):
    def __init__(self, value=None):
        super(ErrorList, self).__init__()
        if value:
            self.add_error(value)

    def add_error(self, error):
        """
        In the case where a list/tuple is passed in this just extends the list
        rather than having nested lists.

        Otherwise, the value is appended.
        """
        if is_non_string_iterable(error) and not isinstance(error, Mapping):
            for value in error:
                self.add_error(value)
        else:
            self.append(error)


class ErrorDict(ErrorCollectionMixin, collections.defaultdict):
    def __init__(self, value=None):
        super(ErrorDict, self).__init__(ErrorList)
        for k, v in (value or {}).items():
            self.add_error(k, v)

    def add_error(self, key, error):
        self[key].add_error(error)


class ValidationError(ValueError):
    def __init__(self, error):
        if not isinstance(error, Mapping) and \
           is_non_string_iterable(error) and \
           len(error) == 1:
            error = error[0]
        self._error = error

        if isinstance(self._error, Mapping):
            self.error_dict = self._error
        elif is_non_string_iterable(self._error):
            self.error_list = self._error

    def __repr__(self):
        return "<type '{0}'>, {1}".format(
            repr(type(self)),
            repr(self.detail),
        )

    def __str__(self):
        return prettify_errors(self._error)

    @property
    def detail(self):
        if isinstance(self._error, six.string_types):
            return [self._error]
        elif isinstance(self._error, Mapping):
            return self._error
        return self._error

    @property
    def messages(self):
        if isinstance(self._error, six.string_types):
            return [self._error]
        elif isinstance(self._error, Mapping):
            return [self._error]
        return self._error


class MultiplePathsFound(ValueError):
    pass


class NoParameterFound(ValueError):
    pass


class MultipleParametersFound(ValueError):
    pass
