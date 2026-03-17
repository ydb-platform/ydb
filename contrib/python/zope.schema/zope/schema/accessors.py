##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""
Field accessors
===============

Accessors are used to model methods used to access data defined by fields.
Accessors are fields that work by decorating existing fields.

To define accessors in an interface, use the accessors function::

  class IMyInterface(Interface):

     getFoo, setFoo = accessors(Text(title=u'Foo', ...))

     getBar = accessors(TextLine(title=u'Foo', readonly=True, ...)


Normally a read accessor and a write accessor are defined.  Only a
read accessor is defined for read-only fields.

Read accessors function as access method specifications and as field
specifications.  Write accessors are solely method specifications.
"""

from zope.interface import implementedBy
from zope.interface import providedBy
from zope.interface.declarations import Declaration
from zope.interface.interface import Method


class FieldReadAccessor(Method):
    """Field read accessor
    """

    def __init__(self, field):
        self.field = field
        Method.__init__(self, '')
        self.__doc__ = 'get %s' % field.__doc__

    # A read field accessor is a method and a field.
    # A read accessor is a decorator of a field, using the given
    # field's properties to provide meta data.

    @property
    def __provides__(self):
        provided = providedBy(self.field)
        implemented = implementedBy(FieldReadAccessor)

        # Declaration.__add__ is not very smart in zope.interface 5.0.0.
        # It's very easy to produce C3 inconsistent orderings using
        # it, because it uses itself plus any new interfaces from the
        # second argument as the ``__bases__``, ignoring their
        # relative order.
        #
        # Here, we can easily work around that. We know that ``field``
        # will be some sub-class of Attribute, just as we are
        # (FieldReadAccessor <- Method <- Attribute). So there will be
        # overlap, and commonly only IMethod would be added to the end
        # of the list of bases; but since IMethod extends IAttribute,
        # having IAttribute earlier in the bases will be inconsistent.
        # The fix here is to remove those duplicates from the first
        # element so that we don't get into that situation.
        provided_list = list(provided)
        for iface in implemented:
            if iface in provided_list:
                provided_list.remove(iface)
        provided = Declaration(*provided_list)
        # pylint:disable=broad-except
        try:
            return provided + implemented
        except BaseException as e:  # pragma: no cover
            # Sadly, zope.interface catches and silently ignores
            # any exceptions raised in ``__providedBy__``,
            # which is the class descriptor that invokes ``__provides__``.
            # So, for example, if we're in strict C3 mode and fail to produce
            # a resolution order, that gets ignored and we fallback to just
            # what's implemented by the class.
            # That's not good. Do our best to propagate the exception by
            # returning it. There will be downstream errors later.
            return e

    def getSignatureString(self):
        return '()'

    def getSignatureInfo(self):
        return {'positional': (),
                'required': (),
                'optional': (),
                'varargs': None,
                'kwargs': None,
                }

    def get(self, object):
        return getattr(object, self.__name__)()

    def query(self, object, default=None):
        try:
            f = getattr(object, self.__name__)
        except AttributeError:
            return default
        else:
            return f()

    def set(self, object, value):
        if self.readonly:
            raise TypeError("Can't set values on read-only fields")
        getattr(object, self.writer.__name__)(value)

    def __getattr__(self, name):
        return getattr(self.field, name)

    def bind(self, object):
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__.update(self.__dict__)
        clone.field = self.field.bind(object)
        return clone


class FieldWriteAccessor(Method):

    def __init__(self, field):
        Method.__init__(self, '')
        self.field = field
        self.__doc__ = 'set %s' % field.__doc__

    def getSignatureString(self):
        return '(newvalue)'

    def getSignatureInfo(self):
        return {'positional': ('newvalue',),
                'required': ('newvalue',),
                'optional': (),
                'varargs': None,
                'kwargs': None,
                }


def accessors(field):
    reader = FieldReadAccessor(field)
    yield reader
    if not field.readonly:
        writer = FieldWriteAccessor(field)
        reader.writer = writer
        yield writer
