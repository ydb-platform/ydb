import json
from collections.abc import Callable
from typing import Any, TypeAlias, TypeVar

textual_types: tuple[Any]

class JSONEncoder(json.JSONEncoder): ...

def dumps(
    s: str,
    _dumps: Callable[..., str],
    cls: JSONEncoder,
    default_kwargs: dict[str, Any],
    **kwargs: Any,
) -> str: ...
def object_hook(o: dict[Any, Any]) -> None: ...
def loads(
    s: str,
    _loads: Callable[[str], Any],
    decode_bytes: bool,
    object_hook: Callable[[dict[Any, Any]], None],
) -> Any: ...

EncoderT: TypeAlias = Callable[[Any], Any]
DecoderT: TypeAlias = Callable[[Any], Any]

_T = TypeVar("_T")
_EncodedT = TypeVar("_EncodedT")

def register_type(
    t: type[_T],
    marker: str | None,
    encoder: Callable[[_T], _EncodedT],
    decoder: Callable[[_EncodedT], _T],
) -> None: ...
