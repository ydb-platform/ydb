"""This module provides top level types for adding type definitions to the
blackboxprotobuf library.
"""

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
    from typing import Any, Dict, List, TypedDict

    # We say messages can have any value
    # Functions we define may have fixed types, but someone could add a type
    # function that outputs any arbitrary object
    Message = Dict[str | int, Any]

    TypeDefDict = Dict[str, "FieldDefDict"]

    FieldDefDict = TypedDict(
        "FieldDefDict",
        {
            "name": str,
            "type": str,
            "message_type_name": str,
            "message_typedef": TypeDefDict,
            "alt_typedefs": Dict[str, str | TypeDefDict],
            "example_value_ignored": Any,
            "seen_repeated": bool,
            "field_order": List[str],
        },
        total=False,
    )
