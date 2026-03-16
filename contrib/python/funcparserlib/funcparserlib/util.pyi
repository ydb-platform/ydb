from typing import TypeVar, Callable, List, Text

_A = TypeVar("_A")

def pretty_tree(
    x: _A, kids: Callable[[_A], List[_A]], show: Callable[[_A], Text]
) -> Text: ...
