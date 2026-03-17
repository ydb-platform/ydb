#  Copyright (c) Kuba Szczodrzyński 2023-1-3.

from dataclasses import Field
from enum import Enum, auto
from typing import IO, Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union

from .utils.misc import dict2str
from .utils.types import FieldTypes


class Container(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self

    def __getattribute__(self, name: str):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return None


class Context(Container):
    class Global(Container):
        io: IO[bytes]
        packing: bool
        unpacking: bool
        sizing: bool
        root: Optional["Context"]
        hooks: List["Hook"]
        tell: Callable[[], int]
        seek: Union[Callable[[int], int], Callable[[int, int], int]]

        def __str__(self) -> str:
            data = dict(self)
            data["pos"] = self.tell()
            data["op"] = "unpacking" if self.unpacking else "packing"
            data.pop("io", None)
            data.pop("packing", None)
            data.pop("unpacking", None)
            data.pop("sizing", None)
            data.pop("root", None)
            data.pop("tell", None)
            data.pop("seek", None)
            return f"({dict2str(data)})"

    class Params(Container):
        config: "Config"
        tell: Callable[[], int]
        seek: Union[Callable[[int], int], Callable[[int, int], int]]
        skip: Callable[[int], int]
        i: int
        item: Any
        self: Any
        kwargs: dict

        def __str__(self) -> str:
            data = dict(self)
            data["pos"] = self.tell()
            data.pop("tell", None)
            data.pop("seek", None)
            data.pop("skip", None)
            return f"({dict2str(data)})"

    _: "Context"
    G: Global
    P: Params
    self: Any

    def __getattribute__(self, name: str) -> Any:
        # get value from this Context, fallback to value from 'self.self'
        try:
            return super(dict, self).__getattribute__(name)
        except AttributeError:
            try:
                _self = super(dict, self).__getattribute__("self")
                return _self.__getattribute__(name)
            except AttributeError:
                return None

    def __setattr__(self, name: str, value: Any) -> None:
        try:
            # set value in 'self.self' if it has the key;
            # otherwise set it directly in Context
            _self = super(dict, self).__getattribute__("self")
            if isinstance(_self, dict):
                if name in _self:
                    _self[name] = value
                else:
                    super(dict, self).__setattr__(name, value)
                return
            _self.__getattribute__(name)
            _self.__setattr__(name, value)
        except AttributeError:
            super(dict, self).__setattr__(name, value)

    def __getitem__(self, name: str) -> Any:
        return self.__getattribute__(name)

    def __setitem__(self, name: str, value: Any) -> None:
        self.__setattr__(name, value)

    def __str__(self) -> str:
        data = dict(self)
        data.pop("_", None)
        return f"Context({dict2str(data)})"


T = TypeVar("T")
V = TypeVar("V")
Eval = Callable[[Context], V]
Value = Union[Eval[V], V]
FormatType = Value[Union[str, int]]
AdapterType = Callable[[Any, Context], Any]
HookType = Callable[[bytes, Context], Optional[bytes]]
ReadType = Callable[[int], bytes]
WriteType = Callable[[bytes], int]
SeekType = Callable[[int, int], int]
TellType = Callable[[], int]


class Adapter:
    # fmt: off
    def encode(self, value: Any, ctx: Context) -> Any: ...
    def decode(self, value: Any, ctx: Context) -> Any: ...
    # fmt: on


class Hook:
    # fmt: off
    def init(self, ctx: Context) -> None: ...
    def update(self, value: bytes, ctx: Context) -> Optional[bytes]: ...
    def read(self, value: bytes, ctx: Context) -> Optional[bytes]: ...
    def write(self, value: bytes, ctx: Context) -> Optional[bytes]: ...
    def end(self, ctx: Context) -> None: ...
    # fmt: on


class IOHook:
    ctx: Optional[Context]
    io_read: Optional[ReadType]
    io_write: Optional[WriteType]
    io_seek: Optional[SeekType]
    io_tell: Optional[TellType]
    hook: Hook

    def __init__(self, hook: Hook = None) -> None:
        self.hook = hook

    def init(self, ctx: Context) -> None:
        if self.hook:
            return self.hook.init(ctx)
        return None

    def read(self, n: int) -> bytes:
        s = self.io_read(n)
        if self.hook:
            ret = self.hook.update(s, self.ctx)
            if ret is not None:
                s = ret
            ret = self.hook.read(s, self.ctx)
            if ret is not None:
                s = ret
        return s

    def write(self, s: bytes) -> int:
        if self.hook:
            ret = self.hook.update(s, self.ctx)
            if ret is not None:
                s = ret
            ret = self.hook.write(s, self.ctx)
            if ret is not None:
                s = ret
        return self.io_write(s)

    def seek(self, offset: int, whence: int) -> int:
        return self.io_seek(offset, whence)

    def tell(self) -> int:
        return self.io_tell()

    def end(self, ctx: Context) -> None:
        if self.hook:
            return self.hook.end(ctx)
        return None


class FieldType(Enum):
    # standard fields
    FIELD = auto()  # field(), subfield(), built(), adapter()
    # special fields
    SEEK = auto()  # seek(), skip()
    PADDING = auto()  # padding(), align()
    ACTION = auto()  # action()
    HOOK = auto()  # hook()
    IO = auto()  # io()
    # wrapper fields
    REPEAT = auto()  # repeat()
    COND = auto()  # cond()
    SWITCH = auto()  # switch()


class FieldMeta(Container):
    validated: bool
    public: bool
    ftype: FieldType
    types: FieldTypes
    # FIELD
    fmt: FormatType
    builder: Value[Any]
    always: bool
    adapter: Adapter
    kwargs: dict
    # SEEK
    offset: Value[int]
    whence: int
    absolute: bool
    # PADDING
    length: Value[int]
    modulus: Value[int]
    pattern: bytes
    check: bool
    # ACTION
    action: Eval[Any]
    # HOOK
    hook: Union[Hook, str]
    end: bool
    # IO
    io: IOHook
    # REPEAT
    base: Field
    count: Value[int]
    when: Eval[bool]
    last: Eval[bool]
    length: Eval[int]
    # COND
    condition: Value[bool]
    if_not: Value[Any]
    # SWITCH
    key: Value[Any]
    fields: Dict[Any, Tuple[Type, Field]]


class Endianness(Enum):
    LITTLE = "<"
    BIG = ">"
    NETWORK = "!"  # == big-endian


LITTLE = Endianness.LITTLE
BIG = Endianness.BIG
NETWORK = Endianness.NETWORK


class Config(Container):
    endianness: Endianness
    padding_pattern: bytes
    padding_check: bool
    repeat_fill: bool
