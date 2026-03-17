#!/usr/bin/env python3
"""
Definition of the beans used to represent the parsed objects

:authors: Thomas Calmant
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

import logging
from enum import IntEnum
from typing import Any, Dict, List, Optional, Set

from ..constants import ClassDescFlags, TypeCode
from ..modifiedutf8 import byte_to_int, decode_modified_utf8
from ..utils import UNICODE_TYPE

# ------------------------------------------------------------------------------

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


class ContentType(IntEnum):
    """
    Types of objects
    """

    INSTANCE = 0
    CLASS = 1
    ARRAY = 2
    STRING = 3
    ENUM = 4
    CLASSDESC = 5
    BLOCKDATA = 6
    EXCEPTIONSTATE = 7


class ClassDataType(IntEnum):
    """
    Class data types
    """

    NOWRCLASS = 0
    WRCLASS = 1
    EXTERNAL_CONTENTS = 2
    OBJECT_ANNOTATION = 3


class ClassDescType(IntEnum):
    """
    Types of class descriptions
    """

    NORMALCLASS = 0
    PROXYCLASS = 1


class FieldType(IntEnum):
    """
    Types of class fields
    """

    BYTE = TypeCode.TYPE_BYTE.value
    CHAR = TypeCode.TYPE_CHAR.value
    DOUBLE = TypeCode.TYPE_DOUBLE.value
    FLOAT = TypeCode.TYPE_FLOAT.value
    INTEGER = TypeCode.TYPE_INTEGER.value
    LONG = TypeCode.TYPE_LONG.value
    SHORT = TypeCode.TYPE_SHORT.value
    BOOLEAN = TypeCode.TYPE_BOOLEAN.value
    ARRAY = TypeCode.TYPE_ARRAY.value
    OBJECT = TypeCode.TYPE_OBJECT.value

    def type_code(self):
        # type: () -> TypeCode
        """
        Converts this FieldType to its matching TypeCode
        """
        return TypeCode(self.value)


class ParsedJavaContent(object):  # pylint:disable=R205
    """
    Generic representation of data parsed from the stream
    """

    def __init__(self, content_type):
        # type: (ContentType) -> None
        self.type = content_type  # type: ContentType
        self.is_exception = False  # type: bool
        self.handle = 0  # type: int

    def __str__(self):
        return "[ParseJavaObject 0x{0:x} - {1}]".format(self.handle, self.type)

    __repr__ = __str__

    def dump(self, indent=0):
        # type: (int) -> str
        """
        Base implementation of a parsed object
        """
        return "\t" * indent + str(self)

    def validate(self):
        """
        Validity check on the object
        """
        pass


class ExceptionState(ParsedJavaContent):
    """
    Representation of a failed parsing
    """

    def __init__(self, exception_object, data):
        # type: (ParsedJavaContent, bytes) -> None
        super(ExceptionState, self).__init__(ContentType.EXCEPTIONSTATE)
        self.exception_object = exception_object
        self.stream_data = data
        self.handle = exception_object.handle

    def dump(self, indent=0):
        # type: (int) -> str
        """
        Returns a dump representation of the exception
        """
        return "\t" * indent + "[ExceptionState {0:x}]".format(self.handle)


class ExceptionRead(Exception):
    """
    Exception used to indicate that an exception object has been parsed
    """

    def __init__(self, content):
        # type: (ParsedJavaContent) -> None
        self.exception_object = content


class JavaString(ParsedJavaContent):
    """
    Represents a Java string
    """

    def __init__(self, handle, data):
        # type: (int, bytes) -> None
        super(JavaString, self).__init__(ContentType.STRING)
        self.handle = handle
        value, length = decode_modified_utf8(data)
        self.value = value  # type: str
        self.length = length  # type: int

    def __repr__(self):
        return repr(self.value)

    def __str__(self):
        return self.value

    def dump(self, indent=0):
        # type: (int) -> str
        """
        Returns a dump representation of the string
        """
        return "\t" * indent + "[String {0:x}: {1}]".format(
            self.handle, repr(self.value)
        )

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return self.value == other


class JavaField:
    """
    Represents a field in a Java class description
    """

    def __init__(self, field_type, name, class_name=None):
        # type: (FieldType, str, Optional[JavaString]) -> None
        self.type = field_type
        self.name = name
        self.class_name = class_name
        self.is_inner_class_reference = False

        if self.class_name:
            self.validate(self.class_name.value)

    def validate(self, java_type):
        # type: (str) -> None
        """
        Validates the type given as parameter
        """
        if self.type == FieldType.OBJECT:
            if not java_type:
                raise ValueError("Class name can't be empty")

            if java_type[0] != "L" or java_type[-1] != ";":
                raise ValueError(
                    "Invalid object field type: {0}".format(java_type)
                )


class JavaClassDesc(ParsedJavaContent):
    """
    Represents the description of a class
    """

    def __init__(self, class_desc_type):
        # type: (ClassDescType) -> None
        super(JavaClassDesc, self).__init__(ContentType.CLASSDESC)

        # Type of class description
        self.class_type = class_desc_type  # type: ClassDescType

        # Class name
        self.name = None  # type: Optional[str]

        # Serial version UID
        self.serial_version_uid = 0  # type: int

        # Description flags byte
        self.desc_flags = 0  # type: int

        # Fields in the class
        self.fields = []  # type: List[JavaField]

        # Inner classes
        self.inner_classes = []  # type: List[JavaClassDesc]

        # List of annotations objects
        self.annotations = []  # type: List[ParsedJavaContent]

        # The super class of this one, if any
        self.super_class = None  # type: Optional[JavaClassDesc]

        # Indicates if it is a super class
        self.is_super_class = False

        # List of the interfaces of the class
        self.interfaces = []  # type: List[str]

        # Set of enum constants
        self.enum_constants = set()  # type: Set[str]

        # Flag to indicate if this is an inner class
        self.is_inner_class = False  # type: bool

        # Flag to indicate if this is a local inner class
        self.is_local_inner_class = False  # type: bool

        # Flag to indicate if this is a static member class
        self.is_static_member_class = False  # type: bool

    def __str__(self):
        return "[classdesc 0x{0:x}: name {1}, uid {2}]".format(
            self.handle, self.name, self.serial_version_uid
        )

    __repr__ = __str__

    def dump(self, indent=0):
        # type: (int) -> str
        """
        Returns a dump representation of the exception
        """
        return "\t" * indent + "[classdesc 0x{0:x}: name {1}, uid {2}]".format(
            self.handle, self.name, self.serial_version_uid
        )

    @property
    def serialVersionUID(self):  # pylint:disable=C0103
        """
        Mimics the javaobj API
        """
        return self.serial_version_uid

    @property
    def flags(self):
        """
        Mimics the javaobj API
        """
        return self.desc_flags

    @property
    def fields_names(self):
        """
        Mimics the javaobj API
        """
        return [field.name for field in self.fields]

    @property
    def fields_types(self):
        """
        Mimics the javaobj API
        """
        return [field.type for field in self.fields]

    @property
    def data_type(self):
        """
        Computes the data type of this class (Write, No Write, Annotation)
        """
        if ClassDescFlags.SC_SERIALIZABLE & self.desc_flags:
            return (
                ClassDataType.WRCLASS
                if (ClassDescFlags.SC_WRITE_METHOD & self.desc_flags)
                else ClassDataType.NOWRCLASS
            )

        if ClassDescFlags.SC_EXTERNALIZABLE & self.desc_flags:
            return (
                ClassDataType.OBJECT_ANNOTATION
                if (ClassDescFlags.SC_WRITE_METHOD & self.desc_flags)
                else ClassDataType.EXTERNAL_CONTENTS
            )

        raise ValueError("Unhandled Class Data Type")

    def is_array_class(self):
        # type: () -> bool
        """
        Determines if this is an array type
        """
        return self.name.startswith("[") if self.name else False

    def get_hierarchy(self, classes):
        # type: (List["JavaClassDesc"]) -> None
        """
        Generates a list of class descriptions in this class's hierarchy, in
        the order described by the Object Stream Serialization Protocol.
        This is the order in which fields are read from the stream.

        :param classes: A list to be filled in with the hierarchy
        """
        if self.super_class is not None:
            if self.super_class.class_type == ClassDescType.PROXYCLASS:
                logging.warning("Hit a proxy class in super class hierarchy")
            else:
                self.super_class.get_hierarchy(classes)

        classes.append(self)

    def validate(self):
        """
        Checks the validity of this class description
        """
        serial_or_extern = (
            ClassDescFlags.SC_SERIALIZABLE | ClassDescFlags.SC_EXTERNALIZABLE
        )
        if (self.desc_flags & serial_or_extern) == 0 and self.fields:
            raise ValueError(
                "Non-serializable, non-externalizable class has fields"
            )

        if self.desc_flags & serial_or_extern == serial_or_extern:
            raise ValueError("Class is both serializable and externalizable")

        if self.desc_flags & ClassDescFlags.SC_ENUM:
            if self.fields or self.interfaces:
                raise ValueError(
                    "Enums shouldn't implement interfaces "
                    "or have non-constant fields"
                )
        else:
            if self.enum_constants:
                raise ValueError(
                    "Non-enum classes shouldn't have enum constants"
                )


class JavaInstance(ParsedJavaContent):
    """
    Represents an instance of Java object
    """

    def __init__(self):
        super(JavaInstance, self).__init__(ContentType.INSTANCE)
        self.classdesc = None  # type: JavaClassDesc
        self.field_data = {}  # type: Dict[JavaClassDesc, Dict[JavaField, Any]]
        self.annotations = (
            {}
        )  # type: Dict[JavaClassDesc, List[ParsedJavaContent]]
        self.is_external_instance = False

    def __str__(self):
        return "[instance 0x{0:x}: type {1}]".format(
            self.handle, self.classdesc.name
        )

    __repr__ = __str__

    def dump(self, indent=0):
        # type: (int) -> str
        """
        Returns a dump representation of the exception
        """
        prefix = "\t" * indent
        sub_prefix = "\t" * (indent + 1)

        dump = [
            prefix
            + "[instance 0x{0:x}: {1:x} / {2}]".format(
                self.handle, self.classdesc.handle, self.classdesc.name
            )
        ]

        for cd, annotations in self.annotations.items():
            dump.append(
                "{0}{1} -- {2} annotations".format(
                    prefix, cd.name, len(annotations)
                )
            )
            for ann in annotations:
                dump.append(sub_prefix + repr(ann))

        for cd, fields in self.field_data.items():
            dump.append(
                "{0}{1} -- {2} fields".format(prefix, cd.name, len(fields))
            )
            for field, value in fields.items():
                if isinstance(value, ParsedJavaContent):
                    if self.handle != 0 and value.handle == self.handle:
                        value_str = "this"
                    else:
                        value_str = "\n" + value.dump(indent + 2)
                else:
                    value_str = repr(value)

                dump.append(
                    "{0}{1} {2}: {3}".format(
                        sub_prefix, field.type.name, field.name, value_str
                    )
                )

        dump.append(prefix + "[/instance 0x{0:x}]".format(self.handle))
        return "\n".join(dump)

    def __getattr__(self, name):
        """
        Returns the field with the given name
        """
        for cd_fields in self.field_data.values():
            for field, value in cd_fields.items():
                if field.name == name:
                    return value

        raise AttributeError(name)

    def get_class(self):
        """
        Returns the class of this instance
        """
        return self.classdesc

    def load_from_blockdata(
        self, parser, reader, indent=0
    ):  # pylint:disable=W0613,R0201
        """
        Reads content stored in a block data.

        This method is called only if the class description has both the
        ``SC_EXTERNALIZABLE`` and ``SC_BLOCK_DATA`` flags set.

        The stream parsing will stop and fail if this method returns False.

        :param parser: The JavaStreamParser in use
        :param reader: The underlying data stream reader
        :param indent: Indentation to use in logs
        :return: True on success, False on error
        """
        return False

    def load_from_instance(self, indent=0):  # pylint:disable=W0613,R0201
        # type: (int) -> bool
        """
        Updates the content of this instance from its parsed fields and
        annotations

        :param indent: Indentation to use in logs
        :return: True on success, False on error (currently ignored)
        """
        return False


class JavaClass(ParsedJavaContent):
    """
    Represents a stored Java class
    """

    def __init__(self, handle, class_desc):
        # type: (int, JavaClassDesc) -> None
        super(JavaClass, self).__init__(ContentType.CLASS)
        self.handle = handle
        self.classdesc = class_desc

    def __str__(self):
        return "[class 0x{0:x}: {1}]".format(self.handle, self.classdesc)

    __repr__ = __str__

    @property
    def name(self):
        """
        Mimics the javaobj API
        """
        return self.classdesc.name


class JavaEnum(ParsedJavaContent):
    """
    Represents an enumeration value
    """

    def __init__(self, handle, class_desc, value):
        # type: (int, JavaClassDesc, JavaString) -> None
        super(JavaEnum, self).__init__(ContentType.ENUM)
        self.handle = handle
        self.classdesc = class_desc
        self.value = value

    def __str__(self):
        return "[Enum 0x{0:x}: {1}]".format(self.handle, self.value)

    __repr__ = __str__

    @property
    def constant(self):
        """
        Mimics the javaobj API
        """
        return self.value


class JavaArray(ParsedJavaContent, list):
    """
    Represents a Java array
    """

    def __init__(self, handle, class_desc, field_type, content):
        # type: (int, JavaClassDesc, FieldType, List[Any]) -> None
        list.__init__(self, content)
        ParsedJavaContent.__init__(self, ContentType.ARRAY)
        self.handle = handle
        self.classdesc = class_desc
        self.field_type = field_type
        self.data = content

    def __str__(self):
        return "[{0}]".format(", ".join(repr(x) for x in self))

    __repr__ = __str__

    def dump(self, indent=0):
        # type: (int) -> str
        """
        Returns a dump representation of the array
        """
        prefix = "\t" * indent
        sub_prefix = "\t" * (indent + 1)
        dump = [
            "{0}[array 0x{1:x}: {2} items - stored as {3}]".format(
                prefix, self.handle, len(self), type(self.data).__name__
            )
        ]
        for x in self:
            if isinstance(x, ParsedJavaContent):
                if self.handle != 0 and x.handle == self.handle:
                    dump.append("this,")
                else:
                    dump.append(x.dump(indent + 1) + ",")
            else:
                dump.append(sub_prefix + repr(x) + ",")
        dump.append(prefix + "[/array 0x{0:x}]".format(self.handle))
        return "\n".join(dump)

    @property
    def _data(self):
        """
        Mimics the javaobj API
        """
        return tuple(self)


class BlockData(ParsedJavaContent):
    """
    Represents a data block
    """

    def __init__(self, data):
        # type: (bytes) -> None
        super(BlockData, self).__init__(ContentType.BLOCKDATA)
        self.data = data

    def __str__(self):
        return "[blockdata 0x{0:x}: {1} bytes]".format(
            self.handle, len(self.data)
        )

    def __repr__(self):
        return repr(self.data)

    def __eq__(self, other):
        if isinstance(other, (str, UNICODE_TYPE)):
            other_data = tuple(ord(x) for x in other)
        elif isinstance(other, bytes):
            other_data = tuple(byte_to_int(x) for x in other)
        else:
            # Can't compare
            return False

        return other_data == tuple(byte_to_int(x) for x in self.data)
