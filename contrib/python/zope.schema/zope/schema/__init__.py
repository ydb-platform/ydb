##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
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
"""Schema package constructor
"""
from zope.schema._bootstrapinterfaces import NO_VALUE
# Field APIs
from zope.schema._field import ASCII
from zope.schema._field import URI
from zope.schema._field import ASCIILine
from zope.schema._field import Bool
from zope.schema._field import Bytes
from zope.schema._field import BytesLine
from zope.schema._field import Choice
from zope.schema._field import Collection
from zope.schema._field import Complex
from zope.schema._field import Container
from zope.schema._field import Date
from zope.schema._field import Datetime
from zope.schema._field import Decimal
from zope.schema._field import Dict
from zope.schema._field import DottedName
from zope.schema._field import Field
from zope.schema._field import Float
from zope.schema._field import FrozenSet
from zope.schema._field import Id
from zope.schema._field import Int
from zope.schema._field import Integral
from zope.schema._field import InterfaceField
from zope.schema._field import Iterable
from zope.schema._field import List
from zope.schema._field import Mapping
from zope.schema._field import MinMaxLen
from zope.schema._field import MutableMapping
from zope.schema._field import MutableSequence
from zope.schema._field import NativeString
from zope.schema._field import NativeStringLine
from zope.schema._field import Number
from zope.schema._field import Object
from zope.schema._field import Orderable
from zope.schema._field import Password
from zope.schema._field import PythonIdentifier
from zope.schema._field import Rational
from zope.schema._field import Real
from zope.schema._field import Sequence
from zope.schema._field import Set
from zope.schema._field import SourceText
from zope.schema._field import Text
from zope.schema._field import TextLine
from zope.schema._field import Time
from zope.schema._field import Timedelta
from zope.schema._field import Tuple
# Schema APIs
from zope.schema._schema import getFieldNames
from zope.schema._schema import getFieldNamesInOrder
from zope.schema._schema import getFields
from zope.schema._schema import getFieldsInOrder
from zope.schema._schema import getSchemaValidationErrors
from zope.schema._schema import getValidationErrors
# Acessor API
from zope.schema.accessors import accessors
# Error API
from zope.schema.interfaces import ValidationError


__all__ = [
    'ASCII',
    'ASCIILine',
    'Bool',
    'Bytes',
    'BytesLine',
    'Choice',
    'Collection',
    'Complex',
    'Container',
    'Date',
    'Datetime',
    'Decimal',
    'Dict',
    'DottedName',
    'Field',
    'Float',
    'FrozenSet',
    'Id',
    'Int',
    'Integral',
    'InterfaceField',
    'Iterable',
    'List',
    'Mapping',
    'MutableMapping',
    'MutableSequence',
    'MinMaxLen',
    'NativeString',
    'NativeStringLine',
    'Number',
    'Object',
    'Orderable',
    'PythonIdentifier',
    'Password',
    'Rational',
    'Real',
    'Set',
    'Sequence',
    'SourceText',
    'Text',
    'TextLine',
    'Time',
    'Timedelta',
    'Tuple',
    'URI',
    'getFields',
    'getFieldsInOrder',
    'getFieldNames',
    'getFieldNamesInOrder',
    'getValidationErrors',
    'getSchemaValidationErrors',
    'accessors',
    'ValidationError',
    'NO_VALUE'
]
