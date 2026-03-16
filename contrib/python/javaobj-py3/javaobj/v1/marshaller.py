#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Provides functions for writing (writing is WIP currently) Java
objects that will be deserialized by ObjectOutputStream. This form of
object representation is a standard data interchange format in Java world.

javaobj module exposes an API familiar to users of the standard library
marshal, pickle and json modules.

See:
http://download.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html

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

# Standard library
import collections
import logging
import struct

try:
    # Python 2
    from StringIO import StringIO as BytesIO
except ImportError:
    # Python 3+
    from io import BytesIO

# Javaobj modules
from .beans import (
    JavaClass,
    JavaString,
    JavaObject,
    JavaByteArray,
    JavaEnum,
    JavaArray,
)
from ..constants import (
    StreamConstants,
    ClassDescFlags,
    TerminalCode,
    TypeCode,
)
from ..utils import (
    log_debug,
    log_error,
    to_bytes,
    BYTES_TYPE,
    UNICODE_TYPE,
)

# ------------------------------------------------------------------------------

__all__ = ("JavaObjectMarshaller",)


# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


class JavaObjectMarshaller:
    """
    Serializes objects into Java serialization format
    """

    def __init__(self, stream=None):
        """
        Sets up members

        :param stream: An output stream
        """
        self.object_stream = stream
        self.object_obj = None
        self.object_transformers = []
        self.references = []

    def add_transformer(self, transformer):
        """
        Appends an object transformer to the serialization process

        :param transformer: An object with a transform(obj) method
        """
        self.object_transformers.append(transformer)

    def dump(self, obj):
        """
        Dumps the given object in the Java serialization format
        """
        self.references = []
        self.object_obj = obj
        self.object_stream = BytesIO()
        self._writeStreamHeader()
        self.writeObject(obj)
        return self.object_stream.getvalue()

    def _writeStreamHeader(self):  # pylint:disable=C0103
        """
        Writes the Java serialization magic header in the serialization stream
        """
        self._writeStruct(
            ">HH",
            4,
            (StreamConstants.STREAM_MAGIC, StreamConstants.STREAM_VERSION),
        )

    def writeObject(self, obj):  # pylint:disable=C0103
        """
        Appends an object to the serialization stream

        :param obj: A string or a deserialized Java object
        :raise RuntimeError: Unsupported type
        """
        log_debug("Writing object of type {0}".format(type(obj).__name__))
        if isinstance(obj, JavaArray):
            # Deserialized Java array
            self.write_array(obj)
        elif isinstance(obj, JavaByteArray):
            # Deserialized Java byte array
            self.write_array(obj)
        elif isinstance(obj, JavaEnum):
            # Deserialized Java Enum
            self.write_enum(obj)
        elif isinstance(obj, JavaObject):
            # Deserialized Java object
            self.write_object(obj)
        elif isinstance(obj, JavaString):
            # Deserialized String
            self.write_string(obj)
        elif isinstance(obj, JavaClass):
            # Java class
            self.write_class(obj)
        elif obj is None:
            # Null
            self.write_null()
        elif type(obj) is str:  # pylint:disable=C0123
            # String value
            self.write_blockdata(obj)
        else:
            # Unhandled type
            raise RuntimeError(
                "Object serialization of type {0} is not "
                "supported.".format(type(obj))
            )

    def _writeStruct(self, unpack, length, args):  # pylint:disable=C0103
        """
        Appends data to the serialization stream

        :param unpack: Struct format string
        :param length: Unused
        :param args: Struct arguments
        """
        ba = struct.pack(unpack, *args)
        self.object_stream.write(ba)

    def _writeString(self, obj, use_reference=True):  # pylint:disable=C0103
        """
        Appends a string to the serialization stream

        :param obj: String to serialize
        :param use_reference: If True, allow writing a reference
        """
        # TODO: Convert to "modified UTF-8"
        # http://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#modified-utf-8
        string = to_bytes(obj, "utf-8")

        if use_reference and isinstance(obj, JavaString):
            try:
                idx = self.references.index(obj)
            except ValueError:
                # First appearance of the string
                self.references.append(obj)
                logging.debug(
                    "*** Adding ref 0x%X for string: %s",
                    len(self.references)
                    - 1
                    + StreamConstants.BASE_REFERENCE_IDX,
                    obj,
                )

                self._writeStruct(">H", 2, (len(string),))
                self.object_stream.write(string)
            else:
                # Write a reference to the previous type
                logging.debug(
                    "*** Reusing ref 0x%X for string: %s",
                    idx + StreamConstants.BASE_REFERENCE_IDX,
                    obj,
                )
                self.write_reference(idx)
        else:
            self._writeStruct(">H", 2, (len(string),))
            self.object_stream.write(string)

    def write_string(self, obj, use_reference=True):
        """
        Writes a Java string with the TC_STRING type marker

        :param obj: The string to print
        :param use_reference: If True, allow writing a reference
        """
        if use_reference and isinstance(obj, JavaString):
            try:
                idx = self.references.index(obj)
            except ValueError:
                # String is not referenced: let _writeString store it
                self._writeStruct(">B", 1, (TerminalCode.TC_STRING,))
                self._writeString(obj, use_reference)
            else:
                # Reuse the referenced string
                logging.debug(
                    "*** Reusing ref 0x%X for String: %s",
                    idx + StreamConstants.BASE_REFERENCE_IDX,
                    obj,
                )
                self.write_reference(idx)
        else:
            # Don't use references
            self._writeStruct(">B", 1, (TerminalCode.TC_STRING,))
            self._writeString(obj, use_reference)

    def write_enum(self, obj):
        """
        Writes an Enum value

        :param obj: A JavaEnum object
        """
        # FIXME: the output doesn't have the same references as the real
        # serializable form
        self._writeStruct(">B", 1, (TerminalCode.TC_ENUM,))

        try:
            idx = self.references.index(obj)
        except ValueError:
            # New reference
            self.references.append(obj)
            logging.debug(
                "*** Adding ref 0x%X for enum: %s",
                len(self.references) - 1 + StreamConstants.BASE_REFERENCE_IDX,
                obj,
            )

            self.write_classdesc(obj.get_class())
        else:
            self.write_reference(idx)

        self.write_string(obj.constant)

    def write_blockdata(self, obj, parent=None):  # pylint:disable=W0613
        """
        Appends a block of data to the serialization stream

        :param obj: String form of the data block
        """
        if isinstance(obj, UNICODE_TYPE):
            # Latin-1: keep bytes as is
            obj = to_bytes(obj, "latin-1")

        length = len(obj)
        if length <= 256:
            # Small block data
            # TC_BLOCKDATA (unsigned byte)<size> (byte)[size]
            self._writeStruct(">B", 1, (TerminalCode.TC_BLOCKDATA,))
            self._writeStruct(">B", 1, (length,))
        else:
            # Large block data
            # TC_BLOCKDATALONG (unsigned int)<size> (byte)[size]
            self._writeStruct(">B", 1, (TerminalCode.TC_BLOCKDATALONG,))
            self._writeStruct(">I", 1, (length,))

        self.object_stream.write(obj)

    def write_null(self):
        """
        Writes a "null" value
        """
        self._writeStruct(">B", 1, (TerminalCode.TC_NULL,))

    def write_object(self, obj, parent=None):
        """
        Writes an object header to the serialization stream

        :param obj: Not yet used
        :param parent: Not yet used
        """
        # Transform object
        for transformer in self.object_transformers:
            tmp_object = transformer.transform(obj)
            if tmp_object is not obj:
                obj = tmp_object
                break

        self._writeStruct(">B", 1, (TerminalCode.TC_OBJECT,))
        cls = obj.get_class()
        self.write_classdesc(cls)

        # Add reference
        self.references.append([])
        logging.debug(
            "*** Adding ref 0x%X for object %s",
            len(self.references) - 1 + StreamConstants.BASE_REFERENCE_IDX,
            obj,
        )

        all_names = collections.deque()
        all_types = collections.deque()
        tmpcls = cls
        while tmpcls:
            all_names.extendleft(reversed(tmpcls.fields_names))
            all_types.extendleft(reversed(tmpcls.fields_types))
            tmpcls = tmpcls.superclass
        del tmpcls

        logging.debug("<=> Field names: %s", all_names)
        logging.debug("<=> Field types: %s", all_types)

        for field_name, field_type in zip(all_names, all_types):
            try:
                logging.debug(
                    "Writing field %s (%s): %s",
                    field_name,
                    field_type,
                    getattr(obj, field_name),
                )
                self._write_value(field_type, getattr(obj, field_name))
            except AttributeError as ex:
                log_error(
                    "No attribute {0} for object {1}\nDir: {2}".format(
                        ex, repr(obj), dir(obj)
                    )
                )
                raise
        del all_names, all_types

        if (
            cls.flags & ClassDescFlags.SC_SERIALIZABLE
            and cls.flags & ClassDescFlags.SC_WRITE_METHOD
            or cls.flags & ClassDescFlags.SC_EXTERNALIZABLE
            and cls.flags & ClassDescFlags.SC_BLOCK_DATA
        ):
            for annotation in obj.annotations:
                log_debug(
                    "Write annotation {0} for {1}".format(
                        repr(annotation), repr(obj)
                    )
                )
                if annotation is None:
                    self.write_null()
                else:
                    self.writeObject(annotation)
            self._writeStruct(">B", 1, (TerminalCode.TC_ENDBLOCKDATA,))

    def write_class(self, obj, parent=None):  # pylint:disable=W0613
        """
        Writes a class to the stream

        :param obj: A JavaClass object
        :param parent:
        """
        self._writeStruct(">B", 1, (TerminalCode.TC_CLASS,))
        self.write_classdesc(obj)

    def write_classdesc(self, obj, parent=None):  # pylint:disable=W0613
        """
        Writes a class description

        :param obj: Class description to write
        :param parent:
        """
        if obj not in self.references:
            # Add reference
            self.references.append(obj)
            logging.debug(
                "*** Adding ref 0x%X for classdesc %s",
                len(self.references) - 1 + StreamConstants.BASE_REFERENCE_IDX,
                obj.name,
            )

            self._writeStruct(">B", 1, (TerminalCode.TC_CLASSDESC,))
            self._writeString(obj.name)
            self._writeStruct(">qB", 1, (obj.serialVersionUID, obj.flags))
            self._writeStruct(">H", 1, (len(obj.fields_names),))

            for field_name, field_type in zip(
                obj.fields_names, obj.fields_types
            ):
                self._writeStruct(
                    ">B", 1, (self._convert_type_to_char(field_type),)
                )
                self._writeString(field_name)
                if ord(field_type[0]) in (
                    TypeCode.TYPE_OBJECT,
                    TypeCode.TYPE_ARRAY,
                ):
                    try:
                        idx = self.references.index(field_type)
                    except ValueError:
                        # First appearance of the type
                        self.references.append(field_type)
                        logging.debug(
                            "*** Adding ref 0x%X for field type %s",
                            len(self.references)
                            - 1
                            + StreamConstants.BASE_REFERENCE_IDX,
                            field_type,
                        )

                        self.write_string(field_type, False)
                    else:
                        # Write a reference to the previous type
                        logging.debug(
                            "*** Reusing ref 0x%X for %s (%s)",
                            idx + StreamConstants.BASE_REFERENCE_IDX,
                            field_type,
                            field_name,
                        )
                        self.write_reference(idx)

            self._writeStruct(">B", 1, (TerminalCode.TC_ENDBLOCKDATA,))
            if obj.superclass:
                self.write_classdesc(obj.superclass)
            else:
                self.write_null()
        else:
            # Use reference
            self.write_reference(self.references.index(obj))

    def write_reference(self, ref_index):
        """
        Writes a reference
        :param ref_index: Local index (0-based) to the reference
        """
        self._writeStruct(
            ">BL",
            1,
            (
                TerminalCode.TC_REFERENCE,
                ref_index + StreamConstants.BASE_REFERENCE_IDX,
            ),
        )

    def write_array(self, obj):
        """
        Writes a JavaArray

        :param obj: A JavaArray object
        """
        classdesc = obj.get_class()
        self._writeStruct(">B", 1, (TerminalCode.TC_ARRAY,))
        self.write_classdesc(classdesc)
        self._writeStruct(">i", 1, (len(obj),))

        # Add reference
        self.references.append(obj)
        logging.debug(
            "*** Adding ref 0x%X for array []",
            len(self.references) - 1 + StreamConstants.BASE_REFERENCE_IDX,
        )

        array_type_code = TypeCode(ord(classdesc.name[0]))
        assert array_type_code == TypeCode.TYPE_ARRAY
        type_code = TypeCode(ord(classdesc.name[1]))

        if type_code == TypeCode.TYPE_OBJECT:
            for o in obj:
                self._write_value(classdesc.name[1:], o)
        elif type_code == TypeCode.TYPE_ARRAY:
            for a in obj:
                self.write_array(a)
        else:
            log_debug("Write array of type {0}".format(chr(type_code.value)))
            for v in obj:
                log_debug("Writing: %s" % v)
                self._write_value(type_code, v)

    def _write_value(self, raw_field_type, value):
        """
        Writes an item of an array

        :param raw_field_type: Value type
        :param value: The value itself
        """
        if isinstance(raw_field_type, (TypeCode, int)):
            field_type = raw_field_type
        else:
            # We don't need details for arrays and objects
            field_type = TypeCode(ord(raw_field_type[0]))

        if field_type == TypeCode.TYPE_BOOLEAN:
            self._writeStruct(">B", 1, (1 if value else 0,))
        elif field_type == TypeCode.TYPE_BYTE:
            self._writeStruct(">b", 1, (value,))
        elif field_type == TypeCode.TYPE_CHAR:
            self._writeStruct(">H", 1, (ord(value),))
        elif field_type == TypeCode.TYPE_SHORT:
            self._writeStruct(">h", 1, (value,))
        elif field_type == TypeCode.TYPE_INTEGER:
            self._writeStruct(">i", 1, (value,))
        elif field_type == TypeCode.TYPE_LONG:
            self._writeStruct(">q", 1, (value,))
        elif field_type == TypeCode.TYPE_FLOAT:
            self._writeStruct(">f", 1, (value,))
        elif field_type == TypeCode.TYPE_DOUBLE:
            self._writeStruct(">d", 1, (value,))
        elif field_type in (TypeCode.TYPE_OBJECT, TypeCode.TYPE_ARRAY):
            if value is None:
                self.write_null()
            elif isinstance(value, JavaEnum):
                self.write_enum(value)
            elif isinstance(value, (JavaArray, JavaByteArray)):
                self.write_array(value)
            elif isinstance(value, JavaObject):
                self.write_object(value)
            elif isinstance(value, JavaString):
                self.write_string(value)
            elif isinstance(value, JavaClass):
                self.write_class(value)
            elif isinstance(value, (BYTES_TYPE, UNICODE_TYPE)):
                self.write_blockdata(value)
            else:
                raise RuntimeError("Unknown typecode: {0}".format(field_type))
        else:
            raise RuntimeError("Unknown typecode: {0}".format(field_type))

    @staticmethod
    def _convert_type_to_char(type_char):
        """
        Converts the given type code to an int

        :param type_char: A type code character
        """
        if isinstance(type_char, TypeCode):
            return type_char.value

        if isinstance(type_char, int):
            return type_char

        if isinstance(type_char, (BYTES_TYPE, UNICODE_TYPE)):
            # Conversion to TypeCode will raise an error if the type
            # is invalid
            return TypeCode(ord(type_char[0])).value

        raise RuntimeError(
            "Typecode {0} ({1}) isn't supported.".format(
                type_char, ord(type_char[0])
            )
        )
