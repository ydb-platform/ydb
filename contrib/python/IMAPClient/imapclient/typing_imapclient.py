from typing import Tuple, Union

_AtomPart = Union[None, int, bytes]
_Atom = Union[_AtomPart, Tuple["_Atom", ...]]
