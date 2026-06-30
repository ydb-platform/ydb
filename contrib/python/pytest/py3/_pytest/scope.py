"""
Scope definition and related utilities.

Those are defined here, instead of in the 'fixtures' module because
their use is spread across many other pytest modules, and centralizing it in 'fixtures'
would cause circular references.

Also this makes the module light to import, as it should.
"""

from enum import Enum
from functools import total_ordering
from typing import Literal
from typing import Optional


_ScopeName = Literal["session", "package", "module", "class", "function"]


@total_ordering
class Scope(Enum):
    """
    Represents one of the possible fixture scopes in pytest.

    Scopes are ordered from lower to higher, that is:

              ->>> higher ->>>

    Function < Class < Module < Package < Session

              <<<- lower  <<<-
    """

    # Scopes need to be listed from lower to higher.
    Function: _ScopeName = "function"
    Class: _ScopeName = "class"
    Module: _ScopeName = "module"
    Package: _ScopeName = "package"
    Session: _ScopeName = "session"

    def next_lower(self) -> "Scope":
        """Return the next lower scope."""
        index = _SCOPE_INDICES[self]
        if index == 0:
            raise ValueError(f"{self} is the lower-most scope")
        return _ALL_SCOPES[index - 1]

    def next_higher(self) -> "Scope":
        """Return the next higher scope."""
        index = _SCOPE_INDICES[self]
        if index == len(_SCOPE_INDICES) - 1:
            raise ValueError(f"{self} is the upper-most scope")
        return _ALL_SCOPES[index + 1]

    def __lt__(self, other: "Scope") -> bool:
        if self == other:
            return False
        self_index = 0
        if self is Scope.Function:
            self_index = 0
        elif self is Scope.Class:
            self_index = 1
        elif self is Scope.Module:
            self_index = 2
        elif self is Scope.Package:
            self_index = 3
        elif self is Scope.Session:
            self_index = 4

        other_index = 0
        if other is Scope.Function:
            other_index = 0
        elif other is Scope.Class:
            other_index = 1
        elif other is Scope.Module:
            other_index = 2
        elif other is Scope.Package:
            other_index = 3
        elif other is Scope.Session:
            other_index = 4

        return self_index < other_index

    @classmethod
    def from_user(
        cls, scope_name: _ScopeName, descr: str, where: Optional[str] = None
    ) -> "Scope":
        """
        Given a scope name from the user, return the equivalent Scope enum. Should be used
        whenever we want to convert a user provided scope name to its enum object.

        If the scope name is invalid, construct a user friendly message and call pytest.fail.
        """
        from _pytest.outcomes import fail

        try:
            # Holding this reference is necessary for mypy at the moment.
            scope = Scope(scope_name)
        except ValueError:
            fail(
                "{} {}got an unexpected scope value '{}'".format(
                    descr, f"from {where} " if where else "", scope_name
                ),
                pytrace=False,
            )
        return scope


_ALL_SCOPES = list(Scope)
_SCOPE_INDICES = {scope: index for index, scope in enumerate(_ALL_SCOPES)}


# Ordered list of scopes which can contain many tests (in practice all except Function).
HIGH_SCOPES = [x for x in Scope if x is not Scope.Function]
