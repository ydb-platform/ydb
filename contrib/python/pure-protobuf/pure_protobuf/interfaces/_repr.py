from typing import Protocol

from typing_extensions import get_args as get_type_args


class Repr(Protocol):
    """
    Provides default implementations for `str(self)` and `repr(self)` to make
    debugging easier.
    """

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        try:
            orig_class = self.__orig_class__  # type: ignore[attr-defined]
        except AttributeError:
            args = ""
        else:
            args = f"[{get_type_args(orig_class)[0]!r}]"
        return f"{type(self).__name__}{args}()"


class ReprWithInner(Repr, Protocol):
    inner: Repr

    def __repr__(self) -> str:
        try:
            orig_class = self.__orig_class__  # type: ignore[attr-defined]
        except AttributeError:
            args = ""
        else:
            args = f"[{get_type_args(orig_class)[0]!r}]"
        return f"{type(self).__name__}{args}({self.inner!r})"
