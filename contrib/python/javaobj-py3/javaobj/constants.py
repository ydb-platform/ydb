#!/usr/bin/env python3
"""
Definition of the constants used in the deserialization process

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

import enum

# ------------------------------------------------------------------------------

__all__ = (
    "PRIMITIVE_TYPES",
    "StreamConstants",
    "TerminalCode",
    "ClassDescFlags",
    "TypeCode",
    "StreamCodeDebug",
)

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


class StreamConstants(enum.IntEnum):
    """
    Basic constants of the stream protocol
    """

    # Magic bytes of any serialized files
    STREAM_MAGIC = 0xACED

    # Only protocol version supported by javaobj
    STREAM_VERSION = 0x05

    # Base index for handles
    BASE_REFERENCE_IDX = 0x7E0000


class TerminalCode(enum.IntEnum):
    """
    Stream type Codes
    """

    TC_NULL = 0x70
    TC_REFERENCE = 0x71
    TC_CLASSDESC = 0x72
    TC_OBJECT = 0x73
    TC_STRING = 0x74
    TC_ARRAY = 0x75
    TC_CLASS = 0x76
    TC_BLOCKDATA = 0x77
    TC_ENDBLOCKDATA = 0x78
    TC_RESET = 0x79
    TC_BLOCKDATALONG = 0x7A
    TC_EXCEPTION = 0x7B
    TC_LONGSTRING = 0x7C
    TC_PROXYCLASSDESC = 0x7D
    TC_ENUM = 0x7E
    # Ignore TC_MAX: we don't use it and it messes with TC_ENUM
    # TC_MAX = 0x7E


class ClassDescFlags(enum.IntEnum):
    """
    Class description flags
    """

    SC_WRITE_METHOD = 0x01  # if SC_SERIALIZABLE
    SC_BLOCK_DATA = 0x08  # if SC_EXTERNALIZABLE
    SC_SERIALIZABLE = 0x02
    SC_EXTERNALIZABLE = 0x04
    SC_ENUM = 0x10


class TypeCode(enum.IntEnum):
    """
    Type definition chars (typecode)
    """

    # Primitive types
    TYPE_BYTE = ord("B")  # 0x42
    TYPE_CHAR = ord("C")  # 0x43
    TYPE_DOUBLE = ord("D")  # 0x44
    TYPE_FLOAT = ord("F")  # 0x46
    TYPE_INTEGER = ord("I")  # 0x49
    TYPE_LONG = ord("J")  # 0x4A
    TYPE_SHORT = ord("S")  # 0x53
    TYPE_BOOLEAN = ord("Z")  # 0x5A
    # Object types
    TYPE_OBJECT = ord("L")  # 0x4C
    TYPE_ARRAY = ord("[")  # 0x5B


# List of the types defined as primitive
PRIMITIVE_TYPES = (
    TypeCode.TYPE_BYTE,
    TypeCode.TYPE_CHAR,
    TypeCode.TYPE_DOUBLE,
    TypeCode.TYPE_FLOAT,
    TypeCode.TYPE_INTEGER,
    TypeCode.TYPE_LONG,
    TypeCode.TYPE_SHORT,
    TypeCode.TYPE_BOOLEAN,
)


class StreamCodeDebug:
    """
    Codes utility methods
    """

    @staticmethod
    def op_id(op_id):
        # type: (int) -> str
        """
        Returns the name of the given OP Code
        :param op_id: OP Code
        :return: Name of the OP Code
        """
        try:
            return TerminalCode(op_id).name
        except ValueError:
            return "<unknown TC:{0}>".format(op_id)

    @staticmethod
    def type_code(type_id):
        # type: (int) -> str
        """
        Returns the name of the given Type Code
        :param type_id: Type code
        :return: Name of the type code
        """
        try:
            return TypeCode(type_id).name
        except ValueError:
            return "<unknown TypeCode:{0}>".format(type_id)

    @staticmethod
    def flags(flags):
        # type: (int) -> str
        """
        Returns the names of the class description flags found in the given
        integer

        :param flags: A class description flag entry
        :return: The flags names as a single string
        """
        names = sorted(key.name for key in ClassDescFlags if key & flags)
        return ", ".join(names)
