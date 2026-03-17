#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Definition of the beans of the v1 parser

:authors: Volodymyr Buell, Thomas Calmant
:license: Apache License 2.0
:version: 0.4.4
:status: Alpha

..

    Copyright 2024 Thomas Calmant

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

from __future__ import absolute_import

from typing import List
import struct

from ..utils import UNICODE_TYPE

# ------------------------------------------------------------------------------

__all__ = (
    "JavaArray",
    "JavaByteArray",
    "JavaClass",
    "JavaEnum",
    "JavaObject",
    "JavaString",
)

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


class JavaClass(object):  # pylint:disable=R0205
    """
    Represents a class in the Java world
    """

    def __init__(self):
        """
        Sets up members
        """
        self.name = None  # type: str
        self.serialVersionUID = None  # type: int  # pylint:disable=C0103
        self.flags = None  # type: int
        self.fields_names = []  # type: List[str]
        self.fields_types = []  # type: List[JavaString]
        self.superclass = None  # type: JavaClass

    def __str__(self):
        """
        String representation of the Java class
        """
        return self.__repr__()

    def __repr__(self):
        """
        String representation of the Java class
        """
        return "[{0:s}:0x{1:X}]".format(self.name, self.serialVersionUID)

    def __eq__(self, other):
        """
        Equality test between two Java classes

        :param other: Other JavaClass to test
        :return: True if both classes share the same fields and name
        """
        if not isinstance(other, type(self)):
            return False

        return (
            self.name == other.name
            and self.serialVersionUID == other.serialVersionUID
            and self.flags == other.flags
            and self.fields_names == other.fields_names
            and self.fields_types == other.fields_types
            and self.superclass == other.superclass
        )


class JavaObject(object):  # pylint:disable=R0205
    """
    Represents a deserialized non-primitive Java object
    """

    def __init__(self):
        """
        Sets up members
        """
        self.classdesc = None  # type: JavaClass
        self.annotations = []

    def get_class(self):
        """
        Returns the JavaClass that defines the type of this object
        """
        return self.classdesc

    def __str__(self):
        """
        String representation
        """
        return self.__repr__()

    def __repr__(self):
        """
        String representation
        """
        name = "UNKNOWN"
        if self.classdesc:
            name = self.classdesc.name
        return "<javaobj:{0}>".format(name)

    def __hash__(self):
        """
        Each JavaObject we load must have a hash method to be accepted in sets
        and alike. The default hash is the memory address of the object.
        """
        return id(self)

    def __eq__(self, other):
        """
        Equality test between two Java classes

        :param other: Other JavaClass to test
        :return: True if both classes share the same fields and name
        """
        if not isinstance(other, type(self)):
            return False

        res = (
            self.classdesc == other.classdesc
            and self.annotations == other.annotations
        )
        if not res:
            return False

        for name in self.classdesc.fields_names:
            if not getattr(self, name) == getattr(other, name):
                return False
        return True


class JavaString(UNICODE_TYPE):
    """
    Represents a Java String
    """

    def __hash__(self):
        return UNICODE_TYPE.__hash__(self)

    def __eq__(self, other):
        if not isinstance(other, UNICODE_TYPE):
            return False
        return UNICODE_TYPE.__eq__(self, other)


class JavaEnum(JavaObject):
    """
    Represents a Java enumeration
    """

    def __init__(self, constant=None):
        super(JavaEnum, self).__init__()
        self.constant = constant


class JavaArray(list, JavaObject):
    """
    Represents a Java Array
    """

    def __init__(self, classdesc=None):
        list.__init__(self)
        JavaObject.__init__(self)
        self.classdesc = classdesc

    def __hash__(self):
        return list.__hash__(self)


class JavaByteArray(JavaObject):
    """
    Represents the special case of Java Array which contains bytes
    """

    def __init__(self, data, classdesc=None):
        JavaObject.__init__(self)
        self._data = struct.unpack("b" * len(data), data)
        self.classdesc = classdesc

    def __str__(self):
        return "JavaByteArray({0})".format(self._data)

    def __getitem__(self, item):
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)
