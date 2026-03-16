#!/usr/bin/python
# -- Content-Encoding: utf-8 --
"""
Provides functions for reading and writing (writing is WIP currently) Java
objects serialized or will be deserialized by ObjectOutputStream. This form of
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
try:
    # Python 2
    from StringIO import StringIO as BytesIO
except ImportError:
    # Python 3+
    from io import BytesIO

# Javaobj modules
from .marshaller import JavaObjectMarshaller
from .unmarshaller import JavaObjectUnmarshaller
from .transformers import DefaultObjectTransformer
from ..utils import java_data_fd

# ------------------------------------------------------------------------------

__all__ = (
    "__version_info__",
    "__version__",
    "JavaObjectMarshaller",
    "JavaObjectUnmarshaller",
    "dumps",
    "load",
    "loads",
)

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


def load(file_object, *transformers, **kwargs):
    """
    Deserializes Java primitive data and objects serialized using
    ObjectOutputStream from a file-like object.

    :param file_object: A file-like object
    :param transformers: Custom transformers to use
    :param ignore_remaining_data: If True, don't log an error when unused
                                  trailing bytes are remaining
    :return: The deserialized object
    """
    # Check file format (uncompress if necessary)
    file_object = java_data_fd(file_object)

    # Read keyword argument
    ignore_remaining_data = kwargs.get("ignore_remaining_data", False)

    marshaller = JavaObjectUnmarshaller(
        file_object, kwargs.get("use_numpy_arrays", False)
    )

    # Add custom transformers first
    for transformer in transformers:
        marshaller.add_transformer(transformer)
    marshaller.add_transformer(DefaultObjectTransformer())

    # Read the file object
    return marshaller.readObject(ignore_remaining_data=ignore_remaining_data)


def loads(string, *transformers, **kwargs):
    """
    Deserializes Java objects and primitive data serialized using
    ObjectOutputStream from a string.

    :param string: A Java data string
    :param transformers: Custom transformers to use
    :param ignore_remaining_data: If True, don't log an error when unused
                                  trailing bytes are remaining
    :return: The deserialized object
    """
    # Reuse the load method (avoid code duplication)
    return load(BytesIO(string), *transformers, **kwargs)


def dumps(obj, *transformers):
    """
    Serializes Java primitive data and objects unmarshaled by load(s) before
    into string.

    :param obj: A Python primitive object, or one loaded using load(s)
    :param transformers: Custom transformers to use
    :return: The serialized data as a string
    """
    marshaller = JavaObjectMarshaller()
    # Add custom transformers
    for transformer in transformers:
        marshaller.add_transformer(transformer)

    return marshaller.dump(obj)
