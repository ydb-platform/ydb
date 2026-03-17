# encoding=utf-8
# Copyright © 2016 Dylan Baker

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without reTextiction, including without limitation the rights
# to use, copy, modify, merge, publish, diTextibute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Stub file for __init__.py."""

from typing import *
try:
    import enum
except ImportError:
    import enum34 as enum  # type: ignore

JSONObject = Dict[Text, Any]
JSONArray = List[Any]
JSONValues = Union[Text, JSONObject, JSONArray, float, int, None]

class Encoder(Generic):
    def encode(self, o: JSONValues) -> Text: ...

def _raise(exc: Exception, *args: Any, **kwargs: Any) -> None: ...

class BaseWriter(object):
    def __init__(self, fd: IO[Text], indent: int, baseindent: int,
                 encoder: Encoder, pretty: bool) -> None:
        self.fd = fd                  # type: IO[Text]
        self.indent = indent          # type: int
        self.baseindent = baseindent  # type: int
        self.encoder = encoder        # type: Encoder
        self.pretty = pretty          # type: bool
    @property
    def comma(self) -> bool: ...
    def raw_write(self, value: JSONValues, indent: bool, newline: bool) -> None: ...

class ObjectWriter(BaseWriter):
    def write_key(self, key: Text) -> None: ...
    def write(self, key: Text, value: JSONValues) -> None: ...

class ArrayWriter(BaseWriter):
    def write(self, key: Text) -> None: ...

class Open(object):
    def __init__(self, intializer: Callable[[None], None],
                 callback: Callable[[None], None]) -> None: ...
    def write(self, key: Text, value: Optional[Text]) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Open: ...

class _CacheChild(object):
    def __init__(self, inst: object, **kwargs: Dict[Text, Any]) -> None:
        self.cached = kwargs  # type: Dict[Text, Any]
        self.inst = inst      # type: object
    def restore(self) -> None: ...

class Object(object):
    def __init__(self, fd: IO[Text], indent: int, baseindnet: int,
                 encoder: Encoder, _indent: bool) -> None: ...
    def subobject(self, key: Text) -> Object: ...
    def subarray(self, key: Text) -> Array: ...
    def write(self, key: Text, value: JSONValues) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Object: ...

class Array(object):
    def __init__(self, fd: IO[Text], indent: int, baseindnet: int,
                 encoder: Encoder, _indent: bool) -> None: ...
    def subobject(self) -> Object: ...
    def subarray(self) -> Array: ...
    def write(self, value: JSONValues) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Array: ...

class Stream(object):
    def __init__(self, jtype: Type, filename: Optional[Text],
                 fd: Optional[IO[Text]], indent: Optional[int],
                 pretty: Optional[bool], encoder: Encoder,
                 close_fd: Optional[bool] = None) -> None: ...
    def subobject(self) -> Object: ...
    def subarray(self) -> Array: ...
    def close(self) -> None: ...
    def __enter__(self) -> Union[Array, Object]: ...
