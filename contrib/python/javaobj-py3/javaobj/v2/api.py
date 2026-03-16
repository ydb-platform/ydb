#!/usr/bin/env python3
"""
Definition of the object transformer API

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

from typing import List, Optional

from ..constants import TypeCode  # pylint:disable=W0611
from .beans import (  # pylint:disable=W0611
    JavaClassDesc,
    JavaInstance,
    ParsedJavaContent,
)
from .stream import DataStreamReader  # pylint:disable=W0611

# ------------------------------------------------------------------------------

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


class IJavaStreamParser:
    """
    API of the Java stream parser
    """

    def run(self):
        # type: () -> List[ParsedJavaContent]
        """
        Parses the input stream
        """
        raise NotImplementedError

    def dump(self, content):
        # type: (List[ParsedJavaContent]) -> str
        """
        Dumps to a string the given objects
        """
        raise NotImplementedError

    def _read_content(self, type_code, block_data, class_desc=None):
        # type: (int, bool, Optional[JavaClassDesc]) -> ParsedJavaContent
        """
        Parses the next content. Use with care (use only in a transformer)
        """


class ObjectTransformer(object):  # pylint:disable=R0205
    """
    Representation of an object transformer
    """

    def create_instance(self, classdesc):  # pylint:disable=W0613,R0201
        # type: (JavaClassDesc) -> Optional[JavaInstance]
        """
        Transforms a parsed Java object into a Python object.

        The result must be a JavaInstance bean, or None if the transformer
        doesn't support this kind of instance.

        :param classdesc: The description of a Java class
        :return: The Python form of the object, or the original JavaObject
        """
        return None

    def load_array(
        self, reader, type_code, size
    ):  # pylint:disable=W0613,R0201
        # type: (DataStreamReader, TypeCode, int) -> Optional[list]
        """
        Loads and returns the content of a Java array, if possible.

        The result of this method must be the content of the array, i.e. a list
        or an array. It will be stored in a JavaArray bean created by the
        parser.

        This method must return None if it can't handle the array.

        :param reader: The data stream reader
        :param type_code: Type of the elements of the array
        :param size: Number of elements in the array
        """
        return None

    def load_custom_writeObject(
        self, parser, reader, name
    ):  # pylint:disable=W0613,R0201
        # type: (IJavaStreamParser, DataStreamReader, str) -> Optional[JavaClassDesc]
        """
        Reads content stored from a custom writeObject.

        This method is called only if the class description has both the
        ``SC_SERIALIZABLE`` and ``SC_WRITE_METHOD`` flags set.

        The stream parsing will stop and fail if this method returns None.

        :param parser: The JavaStreamParser in use
        :param reader: The data stream reader
        :param name: The class description name
        :return: A Java class description, if handled, else None
        """
        return None
