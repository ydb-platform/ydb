from typing import Tuple, Optional, Callable, Iterable, Text, Sequence

_Place = Tuple[int, int]
_Spec = Tuple[Text, Tuple]

class Token:
    type: Text
    value: Text
    start: Optional[_Place]
    end: Optional[_Place]
    name: Text
    def __init__(
        self,
        type: Text,
        value: Text,
        start: Optional[_Place] = ...,
        end: Optional[_Place] = ...,
    ) -> None: ...
    def pformat(self) -> Text: ...

class TokenSpec:
    name: Text
    pattern: Text
    flags: int
    def __init__(self, name: Text, pattern: Text, flags: int = ...) -> None: ...

def make_tokenizer(
    specs: Sequence[TokenSpec | _Spec],
) -> Callable[[Text], Iterable[Token]]: ...

class LexerError(Exception):
    place: Tuple[int, int]
    msg: Text
    def __init__(self, place: _Place, msg: Text) -> None: ...
