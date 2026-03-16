from typing import (
    Any,
    Callable,
    final,
    Final,
    Iterable,
    Literal,
    Optional,
    overload,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

_Data = TypeVar("_Data", contravariant=True)

class _SupportsRead(Protocol):
    def read(self, size: int | None = ...) -> str | bytes: ...

class _SupportsWrite(Protocol[_Data]):
    def write(self, s: _Data) -> Any: ...

_CallbackStr = TypeVar("_CallbackStr", bound=Callable[[str], None])

_CallbackBytes = TypeVar("_CallbackBytes", bound=Callable[[bytes], None])

_SupportsWriteBytes = TypeVar("_SupportsWriteBytes", bound=_SupportsWrite[bytes])

_SupportsWriteStr = TypeVar("_SupportsWriteStr", bound=_SupportsWrite[str])

###############################################################################
### _exports.pyx
###############################################################################

@final
class Options:
    """Customizations for the ``encoder_*(...)`` function family."""

    quotationmark: Final[str] = ...
    tojson: Final[Optional[str]] = ...
    mappingtypes: Final[Tuple[type, ...]] = ...

    def __init__(
        self,
        *,
        quotationmark: Optional[str] = ...,
        tojson: Optional[str] = ...,
        mappingtypes: Optional[Tuple[type, ...]] = ...,
    ) -> None: ...
    def update(
        self,
        *,
        quotationmark: Optional[str] = ...,
        tojson: Optional[str] = ...,
        mappingtypes: Optional[Tuple[type, ...]] = ...,
    ) -> Options:
        """Creates a new Options instance by modifying some members."""

def decode(data: str, maxdepth: Optional[int] = ..., some: bool = ...) -> Any:
    """Decodes JSON5 serialized data from an ``str`` object."""

def decode_latin1(
    data: bytes,
    maxdepth: Optional[int] = ...,
    some: bool = ...,
) -> Any:
    """Decodes JSON5 serialized data from a ``bytes`` object."""

def decode_utf8(
    data: bytes,
    maxdepth: Optional[int] = ...,
    some: bool = ...,
) -> Any:
    """Decodes JSON5 serialized data from a ``bytes`` object."""

def decode_buffer(
    data: bytes,
    maxdepth: Optional[int] = ...,
    some: bool = ...,
    wordlength: Optional[int] = ...,
) -> Any:
    """Decodes JSON5 serialized data from an object that supports the buffer protocol, e.g. bytearray."""

def decode_callback(
    cb: Callable[..., Union[str, bytes, bytearray, int, None]],
    maxdepth: Optional[int] = ...,
    some: bool = ...,
    args: Optional[Iterable[Any]] = ...,
) -> Any:
    """Decodes JSON5 serialized data by invoking a callback."""

def decode_io(
    fp: _SupportsRead,
    maxdepth: Optional[int] = ...,
    some: bool = ...,
) -> Any:
    """Decodes JSON5 serialized data from a file-like object."""

def encode(
    data: Any,
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> str:
    """Serializes a Python object to a JSON5 compatible string."""
    ...

def encode_bytes(
    data: Any,
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> bytes:
    """Serializes a Python object to a JSON5 compatible bytes string."""

@overload
def encode_callback(
    data: Any,
    cb: _CallbackStr,
    supply_bytes: Literal[False] = ...,
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> _CallbackStr:
    """Serializes a Python object into a callback function."""

@overload
def encode_callback(
    data: Any,
    cb: _CallbackBytes,
    supply_bytes: Literal[True],
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> _CallbackBytes: ...
@overload
def encode_io(
    data: Any,
    fp: _SupportsWriteBytes,
    supply_bytes: Literal[True] = ...,
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> _SupportsWriteBytes:
    """Serializes a Python object into a file-object."""

@overload
def encode_io(
    data: Any,
    fp: _SupportsWriteStr,
    supply_bytes: Literal[False],
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> _SupportsWriteStr: ...
def encode_noop(
    data: Any,
    *,
    options: Optional[Options] = ...,
    quotationmark: Optional[str] = ...,
    tojson: Optional[str] = ...,
    mappingtypes: Optional[Tuple[type, ...]] = ...,
) -> bool:
    """Test if the input is serializable."""

###############################################################################
### _legacy.pyx
###############################################################################

def loads(s: str, *, encoding: str = ...) -> Any:
    """Decodes JSON5 serialized data from a string."""

def load(fp: _SupportsRead) -> Any:
    """Decodes JSON5 serialized data from a file-like object."""

def dumps(obj: Any) -> str:
    """Serializes a Python object to a JSON5 compatible string."""

def dump(obj: Any, fp: _SupportsWrite[str]) -> None:
    """Serializes a Python object to a JSON5 compatible string."""

###############################################################################
### _exceptions.pyx
###############################################################################

class Json5Exception(Exception):
    """Base class of any exception thrown by PyJSON5."""

    def __init__(self, message: Optional[str] = ..., *args: Any) -> None: ...
    @property
    def message(self) -> Optional[str]: ...

###############################################################################
### _exceptions_encoder.pyx
###############################################################################

class Json5EncoderException(Json5Exception):
    """Base class of any exception thrown by the serializer."""

@final
class Json5UnstringifiableType(Json5EncoderException):
    """The encoder was not able to stringify the input, or it was told not to by the supplied ``Options``."""

    def __init__(
        self,
        message: Optional[str] = ...,
        unstringifiable: Any = ...,
    ) -> None: ...
    @property
    def unstringifiable(self) -> Any:
        """The value that caused the problem."""

###############################################################################
### _exceptions_decoder.pyx
###############################################################################

class Json5DecoderException(Json5Exception):
    """Base class of any exception thrown by the parser."""

    def __init__(
        self,
        message: Optional[str] = ...,
        result: Any = ...,
        *args: Any,
    ) -> None: ...
    @property
    def result(self) -> Any:
        """Deserialized data up until now."""

@final
class Json5NestingTooDeep(Json5DecoderException):
    """The maximum nesting level on the input data was exceeded."""

@final
class Json5EOF(Json5DecoderException):
    """The input ended prematurely."""

@final
class Json5IllegalCharacter(Json5DecoderException):
    """An unexpected character was encountered."""

    def __init__(
        self,
        message: Optional[str] = ...,
        result: Any = ...,
        character: Optional[str] = ...,
        *args: Any,
    ) -> None: ...
    @property
    def character(self) -> Optional[str]:
        """Illegal character."""

@final
class Json5ExtraData(Json5DecoderException):
    """The input contained extranous data."""

    def __init__(
        self,
        message: Optional[str] = ...,
        result: Any = ...,
        character: Optional[str] = ...,
        *args: Any,
    ) -> None: ...
    @property
    def character(self) -> Optional[str]:
        """Extranous character."""

@final
class Json5IllegalType(Json5DecoderException):
    """The user supplied callback function returned illegal data."""

    def __init__(
        self,
        message: Optional[str] = ...,
        result: Any = ...,
        value: Optional[Any] = ...,
        *args: Any,
    ) -> None: ...
    @property
    def value(self) -> Optional[Any]:
        """Value that caused the problem."""
