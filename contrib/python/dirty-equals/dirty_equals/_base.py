import io
from abc import ABCMeta
from collections.abc import Iterable
from pprint import PrettyPrinter
from typing import TYPE_CHECKING, Any, Generic, Optional, Protocol, TypeVar

from ._utils import Omit

if TYPE_CHECKING:
    from typing import TypeAlias, Union  # noqa: F401

__all__ = 'DirtyEqualsMeta', 'DirtyEquals', 'AnyThing', 'IsOneOf'


class DirtyEqualsMeta(ABCMeta):
    def __eq__(self, other: Any) -> bool:
        if self is other:
            return True

        # this is required as fancy things happen when creating generics which include equals checks, without it,
        # we get some recursive errors
        if self is DirtyEquals or other is Generic or other is Protocol:
            return False
        else:
            try:
                return self() == other
            except TypeError:
                # we don't want to raise a type error here since somewhere deep in pytest it does something like
                # type(a) == type(b), if we raised TypeError we would upset the pytest error message
                return False

    def __or__(self, other: Any) -> 'DirtyOr':  # type: ignore[override]
        return DirtyOr(self, other)

    def __and__(self, other: Any) -> 'DirtyAnd':
        return DirtyAnd(self, other)

    def __invert__(self) -> 'DirtyNot':
        return DirtyNot(self)

    def __hash__(self) -> int:
        return hash(self.__name__)

    def __repr__(self) -> str:
        return self.__name__


T = TypeVar('T')


class DirtyEquals(Generic[T], metaclass=DirtyEqualsMeta):
    """
    Base type for all *dirty-equals* types.
    """

    __slots__ = '_other', '_was_equal', '_repr_args', '_repr_kwargs'

    def __init__(self, *repr_args: Any, **repr_kwargs: Any):
        """
        Args:
            *repr_args: unnamed args to be used in `__repr__`
            **repr_kwargs: named args to be used in `__repr__`
        """
        self._other: Any = None
        self._was_equal: Optional[bool] = None
        self._repr_args: Iterable[Any] = repr_args
        self._repr_kwargs: dict[str, Any] = repr_kwargs

    def equals(self, other: Any) -> bool:
        """
        Abstract method, must be implemented by subclasses.

        `TypeError` and `ValueError` are caught in `__eq__` and indicate `other` is not equals to this type.
        """
        raise NotImplementedError()

    @property
    def value(self) -> T:
        """
        Property to get the value last successfully compared to this object.

        This is seldom very useful, put it's provided for completeness.

        Example of usage:

        ```py title=".values"
        from dirty_equals import IsStr

        token_is_str = IsStr(regex=r't-.+')
        assert 't-123' == token_is_str

        print(token_is_str.value)
        #> t-123
        ```
        """
        if self._was_equal:
            return self._other
        else:
            raise AttributeError('value is not available until __eq__ has been called')

    def __eq__(self, other: Any) -> bool:
        self._other = other
        try:
            self._was_equal = self.equals(other)
        except (TypeError, ValueError):
            self._was_equal = False

        return self._was_equal

    def __ne__(self, other: Any) -> bool:
        # We don't set _was_equal to avoid strange errors in pytest
        self._other = other
        try:
            return not self.equals(other)
        except (TypeError, ValueError):
            return True

    def __or__(self, other: Any) -> 'DirtyOr':
        return DirtyOr(self, other)

    def __and__(self, other: Any) -> 'DirtyAnd':
        return DirtyAnd(self, other)

    def __invert__(self) -> 'DirtyNot':
        return DirtyNot(self)

    def _repr_ne(self) -> str:
        args = [repr(arg) for arg in self._repr_args if arg is not Omit]
        args += [f'{k}={v!r}' for k, v in self._repr_kwargs.items() if v is not Omit]
        return f'{self.__class__.__name__}({", ".join(args)})'

    def __repr__(self) -> str:
        if self._was_equal:
            # if we've got the correct value return it to aid in diffs
            return repr(self._other)
        else:
            # else return something which explains what's going on.
            return self._repr_ne()

    def _pprint_format(self, pprinter: PrettyPrinter, stream: io.StringIO, *args: Any, **kwargs: Any) -> None:
        # pytest diffs use pprint to format objects, so we patch pprint to call this method
        # for DirtyEquals objects. So this method needs to follow the same pattern as __repr__.
        # We check that the protected _format method actually exists
        # to be safe and to make linters happy.
        if self._was_equal and hasattr(pprinter, '_format'):
            pprinter._format(self._other, stream, *args, **kwargs)
        else:
            stream.write(repr(self))  # i.e. self._repr_ne() (for now)


# Patch pprint to call _pprint_format for DirtyEquals objects
# Check that the protected attribute _dispatch exists to be safe and to make linters happy.
# The reason we modify _dispatch rather than _format
# is that pytest sometimes uses a subclass of PrettyPrinter which overrides _format.
if hasattr(PrettyPrinter, '_dispatch'):  # pragma: no branch
    PrettyPrinter._dispatch[DirtyEquals.__repr__] = lambda pprinter, obj, *args, **kwargs: obj._pprint_format(
        pprinter, *args, **kwargs
    )


InstanceOrType: 'TypeAlias' = 'Union[DirtyEquals[Any], DirtyEqualsMeta]'


class DirtyOr(DirtyEquals[Any]):
    def __init__(self, a: 'InstanceOrType', b: 'InstanceOrType', *extra: 'InstanceOrType'):
        self.dirties = (a, b) + extra
        super().__init__()

    def equals(self, other: Any) -> bool:
        return any(d == other for d in self.dirties)

    def _repr_ne(self) -> str:
        return ' | '.join(_repr_ne(d) for d in self.dirties)


class DirtyAnd(DirtyEquals[Any]):
    def __init__(self, a: InstanceOrType, b: InstanceOrType, *extra: InstanceOrType):
        self.dirties = (a, b) + extra
        super().__init__()

    def equals(self, other: Any) -> bool:
        return all(d == other for d in self.dirties)

    def _repr_ne(self) -> str:
        return ' & '.join(_repr_ne(d) for d in self.dirties)


class DirtyNot(DirtyEquals[Any]):
    def __init__(self, subject: InstanceOrType):
        self.subject = subject
        super().__init__()

    def equals(self, other: Any) -> bool:
        return self.subject != other

    def _repr_ne(self) -> str:
        return f'~{_repr_ne(self.subject)}'


def _repr_ne(v: InstanceOrType) -> str:
    if isinstance(v, DirtyEqualsMeta):
        return repr(v)
    else:
        return v._repr_ne()


class AnyThing(DirtyEquals[Any]):
    """
    A type which matches any value. `AnyThing` isn't generally very useful on its own, but can be used within
    other comparisons.

    ```py title="AnyThing"
    from dirty_equals import AnyThing, IsList, IsStrictDict

    assert 1 == AnyThing
    assert 'foobar' == AnyThing
    assert [1, 2, 3] == AnyThing

    assert [1, 2, 3] == IsList(AnyThing, 2, 3)

    assert {'a': 1, 'b': 2, 'c': 3} == IsStrictDict(a=1, b=AnyThing, c=3)
    ```
    """

    def equals(self, other: Any) -> bool:
        return True


class IsOneOf(DirtyEquals[Any]):
    """
    A type which checks that the value is equal to one of the given values.

    Can be useful with boolean operators.
    """

    def __init__(self, expected_value: Any, *more_expected_values: Any):
        """
        Args:
            expected_value: Expected value for equals to return true.
            *more_expected_values: More expected values for equals to return true.

        ```py title="IsOneOf"
        from dirty_equals import Contains, IsOneOf

        assert 1 == IsOneOf(1, 2, 3)
        assert 4 != IsOneOf(1, 2, 3)
        # check that a list either contain 1 or is empty
        assert [1, 2, 3] == Contains(1) | IsOneOf([])
        assert [] == Contains(1) | IsOneOf([])
        ```
        """
        self.expected_values: tuple[Any, ...] = (expected_value,) + more_expected_values
        super().__init__(*self.expected_values)

    def equals(self, other: Any) -> bool:
        return any(other == e for e in self.expected_values)
