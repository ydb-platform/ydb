# Copyright 2023-2025 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import re
from decimal import Decimal
from typing import Optional, Union

import celpy
from celpy import celtypes


class StringFormat:
    """An implementation of string.format() in CEL."""

    def __init__(self):
        self.fmt = None

    def format(self, fmt: celtypes.Value, args: celtypes.Value) -> celpy.Result:
        if not isinstance(fmt, celtypes.StringType):
            return celpy.CELEvalError("format() requires a string as the first argument")
        if not isinstance(args, celtypes.ListType):
            return celpy.CELEvalError("format() requires a list as the second argument")
        # printf style formatting
        i = 0
        j = 0
        result = ""
        while i < len(fmt):
            if fmt[i] != "%":
                result += fmt[i]
                i += 1
                continue

            if i + 1 < len(fmt) and fmt[i + 1] == "%":
                result += "%"
                i += 2
                continue
            if j >= len(args):
                return celpy.CELEvalError(f"index {j} out of range")
            arg = args[j]
            j += 1
            i += 1
            if i >= len(fmt):
                return celpy.CELEvalError("format() incomplete format specifier")
            precision = 6
            if fmt[i] == ".":
                i += 1
                precision = 0
                while i < len(fmt) and fmt[i].isdigit():
                    precision = precision * 10 + int(fmt[i])
                    i += 1
            if i >= len(fmt):
                return celpy.CELEvalError("format() incomplete format specifier")
            if fmt[i] == "f":
                result += self.__format_float(arg, precision)
            elif fmt[i] == "e":
                result += self.__format_exponential(arg, precision)
            elif fmt[i] == "d":
                result += self.__format_int(arg)
            elif fmt[i] == "s":
                result += self.__format_string(arg)
            elif fmt[i] == "x":
                result += self.__format_hex(arg)
            elif fmt[i] == "X":
                result += self.__format_hex(arg).upper()
            elif fmt[i] == "o":
                result += self.__format_oct(arg)
            elif fmt[i] == "b":
                result += self.__format_bin(arg)
            else:
                return celpy.CELEvalError(
                    f'could not parse formatting clause: unrecognized formatting clause "{fmt[i]}"'
                )
            i += 1
        if j < len(args):
            return celpy.CELEvalError("format() too many arguments for format string")

        return celtypes.StringType(result)

    def __validate_number(self, arg: Union[celtypes.DoubleType, celtypes.IntType, celtypes.UintType]) -> Optional[str]:
        if math.isnan(arg):
            return "NaN"
        if math.isinf(arg):
            if arg < 0:
                return "-Infinity"
            return "Infinity"
        return None

    def __format_float(self, arg: celtypes.Value, precision: int) -> str:
        if isinstance(arg, celtypes.DoubleType):
            result = self.__validate_number(arg)
            if result is not None:
                return result
            return f"{arg:.{precision}f}"
        msg = (
            "error during formatting: fixed-point clause can only be used on doubles, was given "
            f"{self.__type_str(type(arg))}"
        )
        raise celpy.CELEvalError(msg)

    def __format_exponential(self, arg: celtypes.Value, precision: int) -> str:
        if isinstance(arg, celtypes.DoubleType):
            result = self.__validate_number(arg)
            if result is not None:
                return result
            return f"{arg:.{precision}e}"
        msg = (
            "error during formatting: scientific clause can only be used on doubles, was given "
            f"{self.__type_str(type(arg))}"
        )
        raise celpy.CELEvalError(msg)

    def __format_int(self, arg: celtypes.Value) -> str:
        if (
            isinstance(arg, celtypes.IntType)
            or isinstance(arg, celtypes.UintType)
            or isinstance(arg, celtypes.DoubleType)
        ):
            result = self.__validate_number(arg)
            if result is not None:
                return result
            return f"{arg}"
        msg = (
            "error during formatting: decimal clause can only be used on integers, was given "
            f"{self.__type_str(type(arg))}"
        )
        raise celpy.CELEvalError(msg)

    def __format_hex(self, arg: celtypes.Value) -> str:
        if isinstance(arg, celtypes.IntType):
            return f"{arg:x}"
        if isinstance(arg, celtypes.UintType):
            return f"{arg:x}"
        if isinstance(arg, celtypes.BytesType):
            return arg.hex()
        if isinstance(arg, celtypes.StringType):
            return arg.encode("utf-8").hex()
        msg = (
            "error during formatting: only integers, byte buffers, and strings can be formatted as hex, was given "
            f"{self.__type_str(type(arg))}"
        )
        raise celpy.CELEvalError(msg)

    def __format_oct(self, arg: celtypes.Value) -> str:
        if isinstance(arg, celtypes.IntType):
            return f"{arg:o}"
        if isinstance(arg, celtypes.UintType):
            return f"{arg:o}"
        msg = (
            "error during formatting: octal clause can only be used on integers, was given "
            f"{self.__type_str(type(arg))}"
        )
        raise celpy.CELEvalError(msg)

    def __format_bin(self, arg: celtypes.Value) -> str:
        if isinstance(arg, celtypes.IntType):
            return f"{arg:b}"
        if isinstance(arg, celtypes.UintType):
            return f"{arg:b}"
        if isinstance(arg, celtypes.BoolType):
            return f"{arg:b}"
        msg = (
            "error during formatting: only integers and bools can be formatted as binary, was given "
            f"{self.__type_str(type(arg))}"
        )

        raise celpy.CELEvalError(msg)

    def __format_string(self, arg: celtypes.Value) -> str:
        if arg is None:
            return "null"
        if isinstance(arg, type):
            return self.__type_str(arg)
        if isinstance(arg, celtypes.BoolType):
            # True -> true
            return str(arg).lower()
        if isinstance(arg, celtypes.BytesType):
            decoded = arg.decode("utf-8", errors="replace")
            # Collapse any contiguous placeholders into one
            return re.sub("\\ufffd+", "\ufffd", decoded)
        if isinstance(arg, celtypes.DoubleType):
            result = self.__validate_number(arg)
            if result is not None:
                return result
            return f"{arg:g}"
        if isinstance(arg, celtypes.DurationType):
            return self.__format_duration(arg)
        if isinstance(arg, celtypes.IntType) or isinstance(arg, celtypes.UintType):
            result = self.__validate_number(arg)
            if result is not None:
                return result
            return f"{arg}"
        if isinstance(arg, celtypes.ListType):
            return self.__format_list(arg)
        if isinstance(arg, celtypes.MapType):
            return self.__format_map(arg)
        if isinstance(arg, celtypes.StringType):
            return f"{arg}"
        if isinstance(arg, celtypes.TimestampType):
            base = arg.isoformat()
            if arg.getMilliseconds() != 0:
                base = arg.isoformat(timespec="milliseconds")
            return base.removesuffix("+00:00") + "Z"
        return "unknown"

    def __format_list(self, arg: celtypes.ListType) -> str:
        result = "["
        for i in range(len(arg)):
            if i > 0:
                result += ", "
            result += self.__format_string(arg[i])
        result += "]"
        return result

    def __format_map(self, arg: celtypes.MapType) -> str:
        m = {}
        for cel_key, cel_val in arg.items():
            key = self.__format_string(cel_key)
            val = self.__format_string(cel_val)
            m[key] = val

        m = dict(sorted(m.items()))

        result = "{"
        for i, (key, val) in enumerate(m.items()):
            if i > 0:
                result += ", "
            result += key + ": " + val

        result += "}"
        return result

    def __format_duration(self, arg: celtypes.DurationType) -> str:
        return f"{arg.seconds + Decimal(arg.microseconds) / Decimal(1_000_000):f}s"

    def __type_str(self, arg: celtypes.Type) -> str:
        if arg is type(None):
            return "null_type"
        if arg is celtypes.BoolType:
            return "bool"
        if arg is celtypes.BytesType:
            return "bytes"
        if arg is celtypes.DoubleType:
            return "double"
        if arg is celtypes.DurationType:
            return "google.protobuf.Duration"
        if arg is celtypes.IntType:
            return "int"
        if arg is celtypes.ListType:
            return "list"
        if arg is celtypes.MapType:
            return "map"
        if arg is celtypes.StringType:
            return "string"
        if arg is celtypes.TimestampType:
            return "google.protobuf.Timestamp"
        if arg is celtypes.UintType:
            return "uint"
        return "unknown"
