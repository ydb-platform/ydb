#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Provides functions for reading Java objects serialized by ObjectOutputStream.
This form of object representation is a standard data interchange format in
Java world.

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
from typing import Any, Union
import os
import struct

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
    StreamCodeDebug,
)
from ..utils import (
    log_debug,
    log_error,
    read_to_str,
    to_unicode,
    unicode_char,
    hexdump,
)

numpy = None  # Imported only when really used

# ------------------------------------------------------------------------------

__all__ = ("JavaObjectUnmarshaller",)

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------

# Convertion of a Java type char to its NumPy equivalent
NUMPY_TYPE_MAP = {
    TypeCode.TYPE_BYTE: "B",
    TypeCode.TYPE_CHAR: "b",
    TypeCode.TYPE_DOUBLE: ">d",
    TypeCode.TYPE_FLOAT: ">f",
    TypeCode.TYPE_INTEGER: ">i",
    TypeCode.TYPE_LONG: ">l",
    TypeCode.TYPE_SHORT: ">h",
    TypeCode.TYPE_BOOLEAN: ">B",
}

# ------------------------------------------------------------------------------


class JavaObjectUnmarshaller:
    """
    Deserializes a Java serialization stream
    """

    def __init__(self, stream, use_numpy_arrays=False):
        """
        Sets up members

        :param stream: An input stream (opened in binary/bytes mode)
        :raise IOError: Invalid input stream
        """
        self.use_numpy_arrays = use_numpy_arrays

        # Numpy array support
        if self.use_numpy_arrays:
            try:
                global numpy
                import numpy as np

                numpy = np
            except ImportError:
                pass

        # Check stream
        if stream is None:
            raise IOError("No input stream given")

        # Prepare the association Terminal Symbol -> Reading method
        self.opmap = {
            TerminalCode.TC_NULL: self.do_null,
            TerminalCode.TC_CLASSDESC: self.do_classdesc,
            TerminalCode.TC_OBJECT: self.do_object,
            TerminalCode.TC_STRING: self.do_string,
            TerminalCode.TC_LONGSTRING: self.do_string_long,
            TerminalCode.TC_ARRAY: self.do_array,
            TerminalCode.TC_CLASS: self.do_class,
            TerminalCode.TC_BLOCKDATA: self.do_blockdata,
            TerminalCode.TC_BLOCKDATALONG: self.do_blockdata_long,
            TerminalCode.TC_REFERENCE: self.do_reference,
            TerminalCode.TC_ENUM: self.do_enum,
            # note that we are reusing do_null:
            TerminalCode.TC_ENDBLOCKDATA: self.do_null,
        }

        # Set up members
        self.current_object = None
        self.reference_counter = 0
        self.references = []
        self.object_transformers = []
        self.object_stream = stream

        # Read the stream header (magic & version)
        self._readStreamHeader()

    def readObject(self, ignore_remaining_data=False):
        """
        Reads an object from the input stream

        :param ignore_remaining_data: If True, don't log an error when
                                      unused trailing bytes are remaining
        :return: The unmarshalled object
        :raise Exception: Any exception that occurred during unmarshalling
        """
        try:
            # TODO: add expects
            _, res = self._read_and_exec_opcode(ident=0)

            position_bak = self.object_stream.tell()
            the_rest = self.object_stream.read()
            if not ignore_remaining_data and len(the_rest) != 0:
                log_error(
                    "Warning!!!!: Stream still has {0} bytes left. "
                    "Enable debug mode of logging to see the hexdump.".format(
                        len(the_rest)
                    )
                )
                log_debug("\n{0}".format(hexdump(the_rest)))
            else:
                log_debug("Java Object unmarshalled successfully!")

            self.object_stream.seek(position_bak)
            return res
        except Exception:
            self._oops_dump_state(ignore_remaining_data)
            raise

    def add_transformer(self, transformer):
        """
        Appends an object transformer to the deserialization process

        :param transformer: An object with a transform(obj) method
        """
        self.object_transformers.append(transformer)

    def _readStreamHeader(self):
        """
        Reads the magic header of a Java serialization stream

        :raise IOError: Invalid magic header (not a Java stream)
        """
        (magic, version) = self._readStruct(">HH")
        if (
            magic != StreamConstants.STREAM_MAGIC
            or version != StreamConstants.STREAM_VERSION
        ):
            raise IOError(
                "The stream is not java serialized object. "
                "Invalid stream header: {0:04X}{1:04X}".format(magic, version)
            )

    def _read_and_exec_opcode(self, ident=0, expect=None):
        """
        Reads the next opcode, and executes its handler

        :param ident: Log identation level
        :param expect: A list of expected opcodes
        :return: A tuple: (opcode, result of the handler)
        :raise IOError: Read opcode is not one of the expected ones
        :raise RuntimeError: Unknown opcode
        """
        position = self.object_stream.tell()
        (opid,) = self._readStruct(">B")
        log_debug(
            "OpCode: 0x{0:X} -- {1} (at offset 0x{2:X})".format(
                opid, StreamCodeDebug.op_id(opid), position
            ),
            ident,
        )

        if expect and opid not in expect:
            raise IOError(
                "Unexpected opcode 0x{0:X} -- {1} "
                "(at offset 0x{2:X})".format(
                    opid, StreamCodeDebug.op_id(opid), position
                )
            )

        try:
            handler = self.opmap[opid]
        except KeyError:
            raise RuntimeError(
                "Unknown OpCode in the stream: 0x{0:X} "
                "(at offset 0x{1:X})".format(opid, position)
            )
        else:
            return opid, handler(ident=ident)

    def _readStruct(self, unpack):
        """
        Reads from the input stream, using struct

        :param unpack: An unpack format string
        :return: The result of struct.unpack (tuple)
        :raise RuntimeError: End of stream reached during unpacking
        """
        length = struct.calcsize(unpack)
        ba = self.object_stream.read(length)

        if len(ba) != length:
            raise RuntimeError(
                "Stream has been ended unexpectedly while unmarshaling."
            )

        return struct.unpack(unpack, ba)

    def _readString(self, length_fmt="H"):
        """
        Reads a serialized string

        :param length_fmt: Structure format of the string length (H or Q)
        :return: The deserialized string
        :raise RuntimeError: Unexpected end of stream
        """
        (length,) = self._readStruct(">{0}".format(length_fmt))
        ba = self.object_stream.read(length)
        return to_unicode(ba)

    def do_classdesc(self, parent=None, ident=0):
        """
        Handles a TC_CLASSDESC opcode

        :param parent:
        :param ident: Log indentation level
        :return: A JavaClass object
        """
        # TC_CLASSDESC className serialVersionUID newHandle classDescInfo
        # classDescInfo:
        #   classDescFlags fields classAnnotation superClassDesc
        # classDescFlags:
        #   (byte)                 // Defined in Terminal Symbols and Constants
        # fields:
        #   (short)<count>  fieldDesc[count]

        # fieldDesc:
        #   primitiveDesc
        #   objectDesc
        # primitiveDesc:
        #   prim_typecode fieldName
        # objectDesc:
        #   obj_typecode fieldName className1
        clazz = JavaClass()
        log_debug("[classdesc]", ident)
        class_name = self._readString()
        clazz.name = class_name
        log_debug("Class name: %s" % class_name, ident)

        # serialVersionUID is a Java (signed) long => 8 bytes
        serialVersionUID, classDescFlags = self._readStruct(">qB")
        clazz.serialVersionUID = serialVersionUID
        clazz.flags = classDescFlags

        self._add_reference(clazz, ident)

        log_debug(
            "Serial: 0x{0:X} / {0:d} - classDescFlags: 0x{1:X} {2}".format(
                serialVersionUID,
                classDescFlags,
                StreamCodeDebug.flags(classDescFlags),
            ),
            ident,
        )
        (length,) = self._readStruct(">H")
        log_debug("Fields num: 0x{0:X}".format(length), ident)

        clazz.fields_names = []
        clazz.fields_types = []
        for fieldId in range(length):
            (typecode,) = self._readStruct(">B")
            field_name = self._readString()
            base_field_type = self._convert_char_to_type(typecode)

            log_debug("> Reading field {0}".format(field_name), ident)

            if base_field_type == TypeCode.TYPE_ARRAY:
                _, field_type = self._read_and_exec_opcode(
                    ident=ident + 1,
                    expect=(TerminalCode.TC_STRING, TerminalCode.TC_REFERENCE),
                )

                if type(field_type) is not JavaString:  # pylint:disable=C0123
                    raise AssertionError(
                        "Field type must be a JavaString, "
                        "not {0}".format(type(field_type))
                    )

            elif base_field_type == TypeCode.TYPE_OBJECT:
                _, field_type = self._read_and_exec_opcode(
                    ident=ident + 1,
                    expect=(TerminalCode.TC_STRING, TerminalCode.TC_REFERENCE),
                )

                if isinstance(field_type, JavaClass):
                    # FIXME: ugly trick
                    field_type = JavaString(field_type.name)

                if type(field_type) is not JavaString:  # pylint:disable=C0123
                    raise AssertionError(
                        "Field type must be a JavaString, "
                        "not {0}".format(type(field_type))
                    )
            else:
                # Convert the TypeCode to its char value
                field_type = JavaString(str(chr(base_field_type.value)))

            log_debug(
                "< FieldName: 0x{0:X} Name:{1} Type:{2} ID:{3}".format(
                    typecode, field_name, field_type, fieldId
                ),
                ident,
            )
            assert field_name is not None
            assert field_type is not None

            clazz.fields_names.append(field_name)
            clazz.fields_types.append(field_type)

        if parent:
            parent.__fields = clazz.fields_names  # pylint:disable=W0212
            parent.__types = clazz.fields_types  # pylint:disable=W0212

        # classAnnotation
        (opid,) = self._readStruct(">B")
        log_debug(
            "OpCode: 0x{0:X} -- {1} (classAnnotation)".format(
                opid, StreamCodeDebug.op_id(opid)
            ),
            ident,
        )
        if opid != TerminalCode.TC_ENDBLOCKDATA:
            raise NotImplementedError("classAnnotation isn't implemented yet")

        # superClassDesc
        log_debug("Reading Super Class of {0}".format(clazz.name), ident)
        _, superclassdesc = self._read_and_exec_opcode(
            ident=ident + 1,
            expect=(
                TerminalCode.TC_CLASSDESC,
                TerminalCode.TC_NULL,
                TerminalCode.TC_REFERENCE,
            ),
        )
        log_debug(
            "Super Class for {0}: {1}".format(clazz.name, str(superclassdesc)),
            ident,
        )
        clazz.superclass = superclassdesc
        return clazz

    def do_blockdata(self, parent=None, ident=0):
        """
        Handles TC_BLOCKDATA opcode

        :param parent:
        :param ident: Log indentation level
        :return: A string containing the block data
        """
        # TC_BLOCKDATA (unsigned byte)<size> (byte)[size]
        log_debug("[blockdata]", ident)
        (length,) = self._readStruct(">B")
        ba = self.object_stream.read(length)

        # Ensure we have an str
        return read_to_str(ba)

    def do_blockdata_long(self, parent=None, ident=0):
        """
        Handles TC_BLOCKDATALONG opcode

        :param parent:
        :param ident: Log indentation level
        :return: A string containing the block data
        """
        # TC_BLOCKDATALONG (int)<size> (byte)[size]
        log_debug("[blockdatalong]", ident)
        (length,) = self._readStruct(">I")
        ba = self.object_stream.read(length)

        # Ensure we have an str
        return read_to_str(ba)

    def do_class(self, parent=None, ident=0):
        """
        Handles TC_CLASS opcode

        :param parent:
        :param ident: Log indentation level
        :return: A JavaClass object
        """
        # TC_CLASS classDesc newHandle
        log_debug("[class]", ident)

        # TODO: what to do with "(ClassDesc)prevObject".
        # (see 3rd line for classDesc:)
        _, classdesc = self._read_and_exec_opcode(
            ident=ident + 1,
            expect=(
                TerminalCode.TC_CLASSDESC,
                TerminalCode.TC_PROXYCLASSDESC,
                TerminalCode.TC_NULL,
                TerminalCode.TC_REFERENCE,
            ),
        )
        log_debug("Classdesc: {0}".format(classdesc), ident)
        self._add_reference(classdesc, ident)
        return classdesc

    def do_object(self, parent=None, ident=0):
        """
        Handles a TC_OBJECT opcode

        :param parent:
        :param ident: Log indentation level
        :return: A JavaClass object
        """
        # TC_OBJECT classDesc newHandle classdata[]  // data for each class
        java_object = JavaObject()
        log_debug("[object]", ident)
        log_debug(
            "java_object.annotations just after instantiation: {0}".format(
                java_object.annotations
            ),
            ident,
        )

        # TODO: what to do with "(ClassDesc)prevObject".
        # (see 3rd line for classDesc:)
        opcode, classdesc = self._read_and_exec_opcode(
            ident=ident + 1,
            expect=(
                TerminalCode.TC_CLASSDESC,
                TerminalCode.TC_PROXYCLASSDESC,
                TerminalCode.TC_NULL,
                TerminalCode.TC_REFERENCE,
            ),
        )
        # self.TC_REFERENCE hasn't shown in spec, but actually is here

        # Create object
        for transformer in self.object_transformers:
            java_object = transformer.create(classdesc, self)
            if java_object is not None:
                break

        # Store classdesc of this object
        java_object.classdesc = classdesc

        # Store the reference
        self._add_reference(java_object, ident)

        # classdata[]

        if (
            classdesc.flags & ClassDescFlags.SC_EXTERNALIZABLE
            and not classdesc.flags & ClassDescFlags.SC_BLOCK_DATA
        ):
            # TODO:
            raise NotImplementedError("externalContents isn't implemented yet")

        if classdesc.flags & ClassDescFlags.SC_SERIALIZABLE:
            # TODO: look at ObjectInputStream.readSerialData()
            # FIXME: Handle the SC_WRITE_METHOD flag

            # create megalist
            tempclass = classdesc
            megalist = []
            megatypes = []
            log_debug("Constructing class...", ident)
            while tempclass:
                log_debug("Class: {0}".format(tempclass.name), ident + 1)
                class_fields_str = " - ".join(
                    " ".join((str(field_type), field_name))
                    for field_type, field_name in zip(
                        tempclass.fields_types, tempclass.fields_names
                    )
                )
                if class_fields_str:
                    log_debug(class_fields_str, ident + 2)

                fieldscopy = tempclass.fields_names[:]
                fieldscopy.extend(megalist)
                megalist = fieldscopy

                fieldscopy = tempclass.fields_types[:]
                fieldscopy.extend(megatypes)
                megatypes = fieldscopy

                tempclass = tempclass.superclass

            log_debug("Values count: {0}".format(len(megalist)), ident)
            log_debug("Prepared list of values: {0}".format(megalist), ident)
            log_debug("Prepared list of types: {0}".format(megatypes), ident)

            for field_name, field_type in zip(megalist, megatypes):
                log_debug(
                    "Reading field: {0} - {1}".format(field_type, field_name)
                )
                res = self._read_value(field_type, ident, name=field_name)
                java_object.__setattr__(field_name, res)

        if (
            classdesc.flags & ClassDescFlags.SC_SERIALIZABLE
            and classdesc.flags & ClassDescFlags.SC_WRITE_METHOD
            or classdesc.flags & ClassDescFlags.SC_EXTERNALIZABLE
            and classdesc.flags & ClassDescFlags.SC_BLOCK_DATA
            or classdesc.superclass is not None
            and classdesc.superclass.flags & ClassDescFlags.SC_SERIALIZABLE
            and classdesc.superclass.flags & ClassDescFlags.SC_WRITE_METHOD
        ):
            # objectAnnotation
            log_debug(
                "java_object.annotations before: {0}".format(
                    java_object.annotations
                ),
                ident,
            )

            while opcode != TerminalCode.TC_ENDBLOCKDATA:
                opcode, obj = self._read_and_exec_opcode(ident=ident + 1)
                # , expect=[self.TC_ENDBLOCKDATA, self.TC_BLOCKDATA,
                # self.TC_OBJECT, self.TC_NULL, self.TC_REFERENCE])
                if opcode != TerminalCode.TC_ENDBLOCKDATA:
                    java_object.annotations.append(obj)

                log_debug("objectAnnotation value: {0}".format(obj), ident)

            log_debug(
                "java_object.annotations after: {0}".format(
                    java_object.annotations
                ),
                ident,
            )

        # Allow extra loading operations
        if hasattr(java_object, "__extra_loading__"):
            log_debug("Java object has extra loading capability.")
            java_object.__extra_loading__(self, ident)

        log_debug(">>> java_object: {0}".format(java_object), ident)
        return java_object

    def do_string(self, parent=None, ident=0):
        """
        Handles a TC_STRING opcode

        :param parent:
        :param ident: Log indentation level
        :return: A string
        """
        log_debug("[string]", ident)
        ba = JavaString(self._readString())
        self._add_reference(ba, ident)
        return ba

    def do_string_long(self, parent=None, ident=0):
        """
        Handles a TC_LONGSTRING opcode

        :param parent:
        :param ident: Log indentation level
        :return: A string
        """
        log_debug("[long string]", ident)
        ba = JavaString(self._readString("Q"))
        self._add_reference(ba, ident)
        return ba

    def do_array(self, parent=None, ident=0):
        """
        Handles a TC_ARRAY opcode

        :param parent:
        :param ident: Log indentation level
        :return: A list of deserialized objects
        """
        # TC_ARRAY classDesc newHandle (int)<size> values[size]
        log_debug("[array]", ident)
        _, classdesc = self._read_and_exec_opcode(
            ident=ident + 1,
            expect=(
                TerminalCode.TC_CLASSDESC,
                TerminalCode.TC_PROXYCLASSDESC,
                TerminalCode.TC_NULL,
                TerminalCode.TC_REFERENCE,
            ),
        )

        array = JavaArray(classdesc)

        self._add_reference(array, ident)

        (size,) = self._readStruct(">i")
        log_debug("size: {0}".format(size), ident)

        array_type_code = TypeCode(ord(classdesc.name[0]))
        assert array_type_code == TypeCode.TYPE_ARRAY
        type_code = TypeCode(ord(classdesc.name[1]))

        if type_code in (TypeCode.TYPE_OBJECT, TypeCode.TYPE_ARRAY):
            for _ in range(size):
                _, res = self._read_and_exec_opcode(ident=ident + 1)
                log_debug("Object value: {0}".format(res), ident)
                array.append(res)
        elif type_code == TypeCode.TYPE_BYTE:
            array = JavaByteArray(self.object_stream.read(size), classdesc)
        elif self.use_numpy_arrays and numpy is not None:
            array = numpy.fromfile(
                self.object_stream,
                dtype=NUMPY_TYPE_MAP[type_code],
                count=size,
            )
        else:
            for _ in range(size):
                res = self._read_value(type_code, ident)
                log_debug("Native value: {0}".format(repr(res)), ident)
                array.append(res)

        return array

    def do_reference(self, parent=None, ident=0):
        """
        Handles a TC_REFERENCE opcode

        :param parent:
        :param ident: Log indentation level
        :return: The referenced object
        """
        (handle,) = self._readStruct(">L")
        log_debug("## Reference handle: 0x{0:X}".format(handle), ident)
        ref = self.references[handle - StreamConstants.BASE_REFERENCE_IDX]
        log_debug("###-> Type: {0} - Value: {1}".format(type(ref), ref), ident)
        return ref

    @staticmethod
    def do_null(parent=None, ident=0):
        """
        Handles a TC_NULL opcode

        :param parent:
        :param ident: Log indentation level
        :return: Always None
        """
        return None

    def do_enum(self, parent=None, ident=0):
        """
        Handles a TC_ENUM opcode

        :param parent:
        :param ident: Log indentation level
        :return: A JavaEnum object
        """
        # TC_ENUM classDesc newHandle enumConstantName
        enum = JavaEnum()
        _, classdesc = self._read_and_exec_opcode(
            ident=ident + 1,
            expect=(
                TerminalCode.TC_CLASSDESC,
                TerminalCode.TC_PROXYCLASSDESC,
                TerminalCode.TC_NULL,
                TerminalCode.TC_REFERENCE,
            ),
        )
        enum.classdesc = classdesc
        self._add_reference(enum, ident)
        (
            _,
            enumConstantName,
        ) = self._read_and_exec_opcode(  # pylint:disable=C0103
            ident=ident + 1,
            expect=(TerminalCode.TC_STRING, TerminalCode.TC_REFERENCE),
        )
        enum.constant = enumConstantName
        return enum

    def _read_value(self, raw_field_type, ident, name=""):
        # type: (Union[bytes, int, TypeCode], int, str) -> Any
        """
        Reads the next value, of the given type

        :param raw_field_type: A serialization typecode
        :param ident: Log indentation
        :param name: Field name (for logs)
        :return: The read value
        :raise RuntimeError: Unknown field type
        """
        if isinstance(raw_field_type, TypeCode):
            field_type = raw_field_type
        elif isinstance(raw_field_type, int):
            field_type = TypeCode(raw_field_type)
        else:
            # We don't need details for arrays and objects
            raw_code = raw_field_type[0]
            if isinstance(raw_code, int):
                field_type = TypeCode(raw_code)
            else:
                field_type = TypeCode(ord(raw_code))

        if field_type == TypeCode.TYPE_BOOLEAN:
            (val,) = self._readStruct(">B")
            res = bool(val)  # type: Any
        elif field_type == TypeCode.TYPE_BYTE:
            (res,) = self._readStruct(">b")
        elif field_type == TypeCode.TYPE_CHAR:
            # TYPE_CHAR is defined by the serialization specification
            # but not used in the implementation, so this is
            # a hypothetical code
            res = unicode_char(self._readStruct(">H")[0])
        elif field_type == TypeCode.TYPE_SHORT:
            (res,) = self._readStruct(">h")
        elif field_type == TypeCode.TYPE_INTEGER:
            (res,) = self._readStruct(">i")
        elif field_type == TypeCode.TYPE_LONG:
            (res,) = self._readStruct(">q")
        elif field_type == TypeCode.TYPE_FLOAT:
            (res,) = self._readStruct(">f")
        elif field_type == TypeCode.TYPE_DOUBLE:
            (res,) = self._readStruct(">d")
        elif field_type in (TypeCode.TYPE_OBJECT, TypeCode.TYPE_ARRAY):
            _, res = self._read_and_exec_opcode(ident=ident + 1)
        else:
            raise RuntimeError("Unknown typecode: {0}".format(field_type))

        log_debug(
            "* {0} {1}: {2}".format(chr(field_type.value), name, repr(res)),
            ident,
        )
        return res

    @staticmethod
    def _convert_char_to_type(type_char):
        # type: (Any) -> TypeCode
        """
        Ensures a read character is a typecode.

        :param type_char: Read typecode
        :return: The typecode as an integer (using ord)
        :raise RuntimeError: Unknown typecode
        """
        typecode = type_char
        if not isinstance(type_char, int):
            typecode = ord(type_char)

        try:
            return TypeCode(typecode)
        except ValueError:
            raise RuntimeError(
                "Typecode {0} ({1}) isn't supported.".format(
                    type_char, typecode
                )
            )

    def _add_reference(self, obj, ident=0):
        """
        Adds a read reference to the marshaler storage

        :param obj: Reference to add
        :param ident: Log indentation level
        """
        log_debug(
            "## New reference handle 0x{0:X}: {1} -> {2}".format(
                len(self.references) + StreamConstants.BASE_REFERENCE_IDX,
                type(obj).__name__,
                repr(obj),
            ),
            ident,
        )
        self.references.append(obj)

    def _oops_dump_state(self, ignore_remaining_data=False):
        """
        Log a deserialization error

        :param ignore_remaining_data: If True, don't log an error when
                                      unused trailing bytes are remaining
        """
        log_error("==Oops state dump" + "=" * (30 - 17))
        log_error("References: {0}".format(self.references))
        log_error(
            "Stream seeking back at -16 byte "
            "(2nd line is an actual position!):"
        )

        # Do not use a keyword argument
        self.object_stream.seek(-16, os.SEEK_CUR)
        position = self.object_stream.tell()
        the_rest = self.object_stream.read()

        if not ignore_remaining_data and len(the_rest) != 0:
            log_error(
                "Warning!!!!: Stream still has {0} bytes left:\n{1}".format(
                    len(the_rest), hexdump(the_rest, position)
                )
            )

        log_error("=" * 30)
