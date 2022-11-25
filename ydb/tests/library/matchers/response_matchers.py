#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
from six import iteritems

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

from ydb.tests.library.common.msgbus_types import MessageBusStatus


class FakeProtobuf(object):
    """
    Fake protobuf-like object with dynamic fields.
    Special created to use in

    >>> d = FakeProtobuf()
    >>> d.val = 100
    >>> str(d.val)
    '<100>'
    >>> d.val2.sub = 100
    >>> str(d.val2.sub)
    '<100>'
    >>> list(map(lambda x: x[0], d))
    ['val', 'val2']
    >>> list(map(lambda x: x[0], d.val2))
    ['sub']

    """

    def __init__(self, **kwargs):
        self.__dict__['_data'] = dict()
        for k, v in iteritems(kwargs):
            if v is not None:
                setattr(self, k, v)

    def __getattr__(self, name):
        ret = self._data.get(name, None)
        if ret is None:
            ret = FakeProtobuf()
            self._data[name] = ret
        return ret

    def __setattr__(self, name, value):
        if isinstance(value, list):
            self._data[name] = value
        else:
            self._data[name] = wrap_matcher(value)

    def __getitem__(self, name):
        return self._data.get(name, None)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return str(map(lambda x: (x[0], str(x[1])), iter(self)))

    def __iter__(self):
        return iter(sorted(self._data.items(), key=lambda x: x[0]))


class AbstractProtobufMatcher(BaseMatcher):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.__expected_protobuf = FakeProtobuf()
        self.__horizontal_line = "\n" + "=" * 60 + "\n"

    @property
    def expected_protobuf(self):
        return self.__expected_protobuf

    def describe_to(self, expected_description):
        expected_description.append_text(self.__horizontal_line)
        self.__describe2('', self.expected_protobuf, expected_description, 0)
        expected_description.append_text(self.__horizontal_line)

    def __describe2(self, attr_name, matcher, expected_description, level):
        prefix = "  " * level
        if isinstance(matcher, BaseMatcher):
            expected_description.append_text(prefix + attr_name + ' = ')
            matcher.describe_to(expected_description)
            expected_description.append_text("\n")
        elif isinstance(matcher, list):
            for item in matcher:
                self.__describe2(attr_name, item, expected_description, level)
                expected_description.append_text("\n")
        else:
            # FakeProtobuf
            expected_description.append_text(prefix + attr_name + " {\n")
            for attr_name, value in matcher:
                self.__describe2(attr_name, value, expected_description, level + 1)
            expected_description.append_text("\n" + prefix + "}")

    def describe_mismatch(self, actual_protobuf, mismatch_description):
        mismatch_description.append_text("Actual protobuf = ")
        mismatch_description.append_text(self.__horizontal_line)
        mismatch_description.append_text(str(actual_protobuf))
        mismatch_description.append_text(self.__horizontal_line)

    def _matches(self, actual_protobuf):
        return self.__match(self.expected_protobuf, actual_protobuf)

    def __match(self, matcher, actual_protobuf):
        if isinstance(matcher, BaseMatcher):
            return matcher.matches(actual_protobuf)
        elif isinstance(matcher, list):
            return len(actual_protobuf) == len(matcher) and \
                all(
                    self.__match(item, actual_protobuf[i])
                    for i, item in enumerate(matcher)
                )
        else:
            return all(
                self.__match(value, getattr(actual_protobuf, name))
                for name, value in matcher
            )


class ProtobufWithStatusMatcher(AbstractProtobufMatcher):
    def __init__(self, status=MessageBusStatus.MSTATUS_OK, error_code=None, error_reason=None):
        super(ProtobufWithStatusMatcher, self).__init__()
        self.expected_protobuf.Status = status
        if error_code is not None:
            self.expected_protobuf.ErrorCode = error_code
        if error_reason is not None:
            self.expected_protobuf.ErrorReason = error_reason


class DynamicFieldsProtobufMatcher(AbstractProtobufMatcher):
    """
    Highly dynamic matcher for Protobuf objects
    You can use it to match any Protobuf object without declaring fields.

    >>> d = DynamicFieldsProtobufMatcher()
    >>> d.val(100).qwe(120)
    DynamicFieldsProtobufMatcher[('qwe', '<120>'), ('val', '<100>')]
    """
    def __init__(self):
        super(DynamicFieldsProtobufMatcher, self).__init__()

    def __getattr__(self, name):
        def closure(val):
            setattr(self.expected_protobuf, name, val)
            return self

        return closure

    def __repr__(self):
        return 'DynamicFieldsProtobufMatcher' + str(self.expected_protobuf)
