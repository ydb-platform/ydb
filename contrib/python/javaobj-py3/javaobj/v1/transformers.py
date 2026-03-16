#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Implementation of the object transformers in v1 parser

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

from typing import Callable, Dict
import functools

from .beans import JavaClass, JavaObject
from .unmarshaller import JavaObjectUnmarshaller
from ..constants import ClassDescFlags, TerminalCode, TypeCode
from ..utils import (
    log_debug,
    log_error,
    to_bytes,
    read_struct,
    read_string,
)


__all__ = ("DefaultObjectTransformer",)


class DefaultObjectTransformer(object):  # pylint:disable=R0205
    """
    Default transformer for the deserialized objects.
    Converts JavaObject objects to Python types (maps, lists, ...)
    """

    class JavaList(list, JavaObject):
        """
        Python-Java list bridge type
        """

        def __init__(self, unmarshaller):
            # type: (JavaObjectUnmarshaller) -> None
            list.__init__(self)
            JavaObject.__init__(self)

        def __hash__(self):
            return list.__hash__(self)

        def __extra_loading__(self, unmarshaller, ident=0):
            # type: (JavaObjectUnmarshaller, int) -> None
            """
            Loads the content of the map, written with a custom implementation
            """
            # Lists have their content in there annotations
            self.extend(self.annotations[1:])

    @functools.total_ordering
    class JavaPrimitiveClass(JavaObject):
        """
        Parent of Java classes matching a primitive (Bool, Integer, Long, ...)
        """

        def __init__(self, unmarshaller):
            JavaObject.__init__(self)
            self.value = None

        def __str__(self):
            return str(self.value)

        def __repr__(self):
            return repr(self.value)

        def __hash__(self):
            return hash(self.value)

        def __eq__(self, other):
            return self.value == other

        def __lt__(self, other):
            return self.value < other

    class JavaBool(JavaPrimitiveClass):
        def __bool__(self):
            return self.value

    class JavaInt(JavaPrimitiveClass):
        def __int__(self):
            return self.value

    class JavaMap(dict, JavaObject):
        """
        Python-Java dictionary/map bridge type
        """

        def __init__(self, unmarshaller):
            # type: (JavaObjectUnmarshaller) -> None
            dict.__init__(self)
            JavaObject.__init__(self)

        def __hash__(self):
            return dict.__hash__(self)

        def __extra_loading__(self, unmarshaller, ident=0):
            # type: (JavaObjectUnmarshaller, int) -> None
            """
            Loads the content of the map, written with a custom implementation
            """
            # Group annotation elements 2 by 2
            args = [iter(self.annotations[1:])] * 2
            for key, value in zip(*args):
                self[key] = value

    class JavaLinkedHashMap(JavaMap):
        def __extra_loading__(self, unmarshaller, ident=0):
            # type: (JavaObjectUnmarshaller, int) -> None
            """
            Loads the content of the map, written with a custom implementation
            """
            # Ignore the blockdata opid
            (opid,) = unmarshaller._readStruct(">B")
            if opid != ClassDescFlags.SC_BLOCK_DATA:
                raise ValueError("Start of block data not found")

            # Read HashMap fields
            self.buckets = unmarshaller._read_value(
                TypeCode.TYPE_INTEGER, ident
            )
            self.size = unmarshaller._read_value(TypeCode.TYPE_INTEGER, ident)

            # Read entries
            for _ in range(self.size):
                key = unmarshaller._read_and_exec_opcode()[1]
                value = unmarshaller._read_and_exec_opcode()[1]
                self[key] = value

            # Ignore the end of the blockdata
            unmarshaller._read_and_exec_opcode(
                ident, [TerminalCode.TC_ENDBLOCKDATA]
            )

            # Ignore the trailing 0
            (opid,) = unmarshaller._readStruct(">B")
            if opid != 0:
                raise ValueError("Should find 0x0, got {0:x}".format(opid))

    class JavaSet(set, JavaObject):
        """
        Python-Java set bridge type
        """

        def __init__(self, unmarshaller):
            # type: (JavaObjectUnmarshaller) -> None
            set.__init__(self)
            JavaObject.__init__(self)

        def __hash__(self):
            return set.__hash__(self)

        def __extra_loading__(self, unmarshaller, ident=0):
            # type: (JavaObjectUnmarshaller, int) -> None
            """
            Loads the content of the map, written with a custom implementation
            """
            self.update(self.annotations[1:])

    class JavaTreeSet(JavaSet):
        def __extra_loading__(self, unmarshaller, ident=0):
            # type: (JavaObjectUnmarshaller, int) -> None
            """
            Loads the content of the map, written with a custom implementation
            """
            # Annotation[1] == size of the set
            self.update(self.annotations[2:])

    class JavaTime(JavaObject):
        """
        Represents the classes found in the java.time package

        The semantic of the fields depends on the type of time that has been
        parsed
        """

        DURATION_TYPE = 1
        INSTANT_TYPE = 2
        LOCAL_DATE_TYPE = 3
        LOCAL_TIME_TYPE = 4
        LOCAL_DATE_TIME_TYPE = 5
        ZONE_DATE_TIME_TYPE = 6
        ZONE_REGION_TYPE = 7
        ZONE_OFFSET_TYPE = 8
        OFFSET_TIME_TYPE = 9
        OFFSET_DATE_TIME_TYPE = 10
        YEAR_TYPE = 11
        YEAR_MONTH_TYPE = 12
        MONTH_DAY_TYPE = 13
        PERIOD_TYPE = 14

        def __init__(self, unmarshaller):
            # type: (JavaObjectUnmarshaller) -> None
            JavaObject.__init__(self)
            self.type = -1
            self.year = None
            self.month = None
            self.day = None
            self.hour = None
            self.minute = None
            self.second = None
            self.nano = None
            self.offset = None
            self.zone = None

            self.time_handlers = {
                self.DURATION_TYPE: self.do_duration,
                self.INSTANT_TYPE: self.do_instant,
                self.LOCAL_DATE_TYPE: self.do_local_date,
                self.LOCAL_DATE_TIME_TYPE: self.do_local_date_time,
                self.LOCAL_TIME_TYPE: self.do_local_time,
                self.ZONE_DATE_TIME_TYPE: self.do_zoned_date_time,
                self.ZONE_OFFSET_TYPE: self.do_zone_offset,
                self.ZONE_REGION_TYPE: self.do_zone_region,
                self.OFFSET_TIME_TYPE: self.do_offset_time,
                self.OFFSET_DATE_TIME_TYPE: self.do_offset_date_time,
                self.YEAR_TYPE: self.do_year,
                self.YEAR_MONTH_TYPE: self.do_year_month,
                self.MONTH_DAY_TYPE: self.do_month_day,
                self.PERIOD_TYPE: self.do_period,
            }

        def __str__(self):
            return (
                "JavaTime(type=0x{s.type}, "
                "year={s.year}, month={s.month}, day={s.day}, "
                "hour={s.hour}, minute={s.minute}, second={s.second}, "
                "nano={s.nano}, offset={s.offset}, zone={s.zone})"
            ).format(s=self)

        def __extra_loading__(self, unmarshaller, ident=0):
            # type: (JavaObjectUnmarshaller, int) -> None
            """
            Loads the content of the map, written with a custom implementation
            """
            # Convert back annotations to bytes
            # latin-1 is used to ensure that bytes are kept as is
            content = to_bytes(self.annotations[0], "latin1")
            (self.type,), content = read_struct(content, ">b")

            try:
                self.time_handlers[self.type](unmarshaller, content)
            except KeyError as ex:
                log_error("Unhandled kind of time: {}".format(ex))

        def do_duration(self, unmarshaller, data):
            (self.second, self.nano), data = read_struct(data, ">qi")
            return data

        def do_instant(self, unmarshaller, data):
            (self.second, self.nano), data = read_struct(data, ">qi")
            return data

        def do_local_date(self, unmarshaller, data):
            (self.year, self.month, self.day), data = read_struct(data, ">ibb")
            return data

        def do_local_time(self, unmarshaller, data):
            (hour,), data = read_struct(data, ">b")
            minute = 0
            second = 0
            nano = 0

            if hour < 0:
                hour = ~hour
            else:
                (minute,), data = read_struct(data, ">b")
                if minute < 0:
                    minute = ~minute
                else:
                    (second,), data = read_struct(data, ">b")
                    if second < 0:
                        second = ~second
                    else:
                        (nano,), data = read_struct(data, ">i")

            self.hour = hour
            self.minute = minute
            self.second = second
            self.nano = nano
            return data

        def do_local_date_time(self, unmarshaller, data):
            data = self.do_local_date(unmarshaller, data)
            data = self.do_local_time(unmarshaller, data)
            return data

        def do_zoned_date_time(self, unmarshaller, data):
            data = self.do_local_date_time(unmarshaller, data)
            data = self.do_zone_offset(unmarshaller, data)
            data = self.do_zone_region(unmarshaller, data)
            return data

        def do_zone_offset(self, unmarshaller, data):
            (offset_byte,), data = read_struct(data, ">b")
            if offset_byte == 127:
                (self.offset,), data = read_struct(data, ">i")
            else:
                self.offset = offset_byte * 900
            return data

        def do_zone_region(self, unmarshaller, data):
            self.zone, data = read_string(data)
            return data

        def do_offset_time(self, unmarshaller, data):
            data = self.do_local_time(unmarshaller, data)
            data = self.do_zone_offset(unmarshaller, data)
            return data

        def do_offset_date_time(self, unmarshaller, data):
            data = self.do_local_date_time(unmarshaller, data)
            data = self.do_zone_offset(unmarshaller, data)
            return data

        def do_year(self, unmarshaller, data):
            (self.year,), data = read_struct(data, ">i")
            return data

        def do_year_month(self, unmarshaller, data):
            (self.year, self.month), data = read_struct(data, ">ib")
            return data

        def do_month_day(self, unmarshaller, data):
            (self.month, self.day), data = read_struct(data, ">bb")
            return data

        def do_period(self, unmarshaller, data):
            (self.year, self.month, self.day), data = read_struct(data, ">iii")
            return data

    TYPE_MAPPER = {
        "java.util.ArrayList": JavaList,
        "java.util.LinkedList": JavaList,
        "java.util.HashMap": JavaMap,
        "java.util.LinkedHashMap": JavaLinkedHashMap,
        "java.util.TreeMap": JavaMap,
        "java.util.HashSet": JavaSet,
        "java.util.LinkedHashSet": JavaSet,
        "java.util.TreeSet": JavaTreeSet,
        "java.time.Ser": JavaTime,
        "java.lang.Boolean": JavaBool,
        "java.lang.Integer": JavaInt,
        "java.lang.Long": JavaInt,
    }  # type: Dict[str, Callable[[JavaObjectUnmarshaller], JavaObject]]

    def create(self, classdesc, unmarshaller):
        # type: (JavaClass, JavaObjectUnmarshaller) -> JavaObject
        """
        Transforms a deserialized Java object into a Python object

        :param classdesc: The description of a Java class
        :return: The Python form of the object, or the original JavaObject
        """
        try:
            mapped_type = self.TYPE_MAPPER[classdesc.name]
        except KeyError:
            # Return a JavaObject by default
            return JavaObject()
        else:
            log_debug("---")
            log_debug(classdesc.name)
            log_debug("---")

            java_object = mapped_type(unmarshaller)

            log_debug(">>> java_object: {0}".format(java_object))
            return java_object
