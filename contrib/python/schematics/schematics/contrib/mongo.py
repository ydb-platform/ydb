"""This module contains fields that depend on importing `bson`. `bson` is
a part of the pymongo distribution.
"""

from __future__ import unicode_literals, absolute_import

import bson

from ..common import *
from ..translator import _
from ..types import BaseType
from ..exceptions import ConversionError

__all__ = ['ObjectIdType']


class ObjectIdType(BaseType):

    """An field wrapper around MongoDB ObjectIds.  It is correct to say they're
    bson fields, but I am unaware of bson being used outside MongoDB.

    `auto_fill` is disabled by default for ObjectIdType's as they are
    typically obtained after a successful save to Mongo.
    """

    MESSAGES = {
        'convert': _("Couldn't interpret value as an ObjectId."),
    }

    def __init__(self, auto_fill=False, **kwargs):
        self.auto_fill = auto_fill
        super(ObjectIdType, self).__init__(**kwargs)

    def to_native(self, value, context=None):
        if not isinstance(value, bson.objectid.ObjectId):
            try:
                value = bson.objectid.ObjectId(str(value))
            except bson.objectid.InvalidId:
                raise ConversionError(self.messages['convert'])
        return value

    def to_primitive(self, value, context=None):
        return str(value)

if PY2:
    # Python 2 names cannot be unicode
    __all__ = [n.encode('ascii') for n in __all__]
