#!/usr/bin/env python3
"""
Mimics the core API with the new deserializer
"""

from __future__ import absolute_import

from typing import IO, Any  # pylint:disable=W0611

try:
    # Python 2
    from StringIO import StringIO as BytesIO
except ImportError:
    # Python 3+
    from io import BytesIO

from ..utils import java_data_fd
from .api import ObjectTransformer  # pylint:disable=W0611
from .core import JavaStreamParser
from .transformers import DefaultObjectTransformer, NumpyArrayTransformer

# ------------------------------------------------------------------------------

# Module version
__version_info__ = (0, 4, 4)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

# ------------------------------------------------------------------------------


def load(file_object, *transformers, **kwargs):
    # type: (IO[bytes], ObjectTransformer, Any) -> Any
    """
    Deserializes Java primitive data and objects serialized using
    ObjectOutputStream from a file-like object.

    :param file_object: A file-like object
    :param transformers: Custom transformers to use
    :return: The deserialized object
    """
    # Check file format (uncompress if necessary)
    file_object = java_data_fd(file_object)

    # Ensure we have the default object transformer
    all_transformers = list(transformers)
    for t in all_transformers:
        if isinstance(t, DefaultObjectTransformer):
            break
    else:
        all_transformers.append(DefaultObjectTransformer())

    if kwargs.get("use_numpy_arrays", False):
        # Use the numpy array transformer if requested
        all_transformers.append(NumpyArrayTransformer())

    # Parse the object(s)
    parser = JavaStreamParser(file_object, all_transformers)
    contents = parser.run()

    if len(contents) == 0:
        # Nothing was parsed, but no error
        return None
    elif len(contents) == 1:
        # Return the only object as is
        return contents[0]
    else:
        # Returns all objects if they are more than one
        return contents


def loads(data, *transformers, **kwargs):
    # type: (bytes, ObjectTransformer, Any) -> Any
    """
    Deserializes Java objects and primitive data serialized using
    ObjectOutputStream from bytes.

    :param data: A Java data string
    :param transformers: Custom transformers to use
    :param ignore_remaining_data: If True, don't log an error when unused
                                  trailing bytes are remaining
    :return: The deserialized object
    """
    return load(BytesIO(data), *transformers, **kwargs)
