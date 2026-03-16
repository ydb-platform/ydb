"""Exception classes for BlackboxProtobuf"""

# Copyright (c) 2018-2024 NCC Group Plc
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import six

if six.PY3:
    from typing import Any, Optional, List


class BlackboxProtobufException(Exception):
    """Base class for excepions raised by Blackbox Protobuf"""

    def __init__(self, message, path=None, *args):
        # type: (str, Optional[List[str]], Any) -> None
        self.path = path
        super(BlackboxProtobufException, self).__init__(message, *args)

    def set_path(self, path):
        # type: (BlackboxProtobufException, List[str]) -> None
        if self.path is None:
            self.path = path


class TypedefException(BlackboxProtobufException):
    """Thrown when an error is identified in the type definition, such as
    conflicting or inconsistent values."""

    def __str__(self):
        # type: (TypedefException) -> str
        message = super(TypedefException, self).__str__()
        if self.path is not None:
            message = (
                "Encountered error within typedef for field %s: "
                % "->".join(map(str, self.path))
            ) + message
        else:
            message = ("Encountered error within typedef: ") + message
        return message


class EncoderException(BlackboxProtobufException, ValueError):
    """Thrown when there is an error encoding a dictionary to a type definition"""

    def __str__(self):
        # type: (EncoderException) -> str
        message = super(EncoderException, self).__str__()
        if self.path is not None:
            message = (
                "Encountered error encoding field %s: " % "->".join(map(str, self.path))
            ) + message
        else:
            message = ("Encountered error encoding message: ") + message
        return message


class DecoderException(BlackboxProtobufException, ValueError):
    """Thrown when there is an error decoding a bytestring to a dictionary"""

    def __str__(self):
        # type: (DecoderException) -> str
        message = super(DecoderException, self).__str__()
        if self.path is not None:
            message = (
                "Encountered error decoding field %s: " % "->".join(map(str, self.path))
            ) + message
        else:
            message = ("Encountered error decoding message: ") + message
        return message


class ProtofileException(BlackboxProtobufException):
    def __init__(self, message, path=None, filename=None, *args):
        # type: (ProtofileException, str, Optional[List[str]], Optional[str], Any) -> None
        self.filename = filename
        super(BlackboxProtobufException, self).__init__(message, path, *args)

    def __str__(self):
        # type: (ProtofileException) -> str
        message = super(ProtofileException, self).__str__()
        if self.path is not None:
            message = (
                "Encountered error within protofile %s for field %s: "
                % (self.filename, "->".join(map(str, self.path)))
            ) + message
        else:
            message = (
                "Encountered error within protofile %s: " % self.filename
            ) + message

        return message
