# -*- mode: python; coding: utf-8 -*-
from __future__ import absolute_import
import six


class FalseMeta(type):
    def __bool__(self):  # pragma: no cover
        return False

    def __cmp__(self, other):  # pragma: no cover
        return -1

    __nonzero__ = __bool__


class UnknownLength(six.with_metaclass(FalseMeta, object)):
    pass


class Undefined(six.with_metaclass(FalseMeta, object)):
    pass
