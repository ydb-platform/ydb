import re
from re import Pattern
from typing import Any, Literal, Optional, TypeVar, Union

from ._base import DirtyEquals
from ._utils import Omit, plain_repr

T = TypeVar('T', str, bytes)

__all__ = 'IsStr', 'IsBytes', 'IsAnyStr'


class IsAnyStr(DirtyEquals[T]):
    """
    Comparison of `str` or `bytes` objects.

    This class allow comparison with both `str` and `bytes` but is subclassed
    by [`IsStr`][dirty_equals.IsStr] and [`IsBytes`][dirty_equals.IsBytes] which restrict comparison to
    `str` or `bytes` respectively.
    """

    expected_types: tuple[type[Any], ...] = (str, bytes)

    def __init__(
        self,
        *,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        case: Literal['upper', 'lower', None] = None,
        regex: Union[None, T, Pattern[T]] = None,
        regex_flags: int = 0,
    ):
        """
        Args:
            min_length: minimum length of the string/bytes
            max_length: maximum length of the string/bytes
            case: check case of the string/bytes
            regex: regular expression to match the string/bytes with, `re.fullmatch` is used.
                This can be a compiled regex, or a string or bytes.
            regex_flags: optional flags for the regular expression

        Examples:
        ```py title="IsAnyStr"
        from dirty_equals import IsAnyStr

        assert 'foobar' == IsAnyStr()
        assert b'foobar' == IsAnyStr()
        assert 123 != IsAnyStr()
        assert 'foobar' == IsAnyStr(regex='foo...')
        assert 'foobar' == IsAnyStr(regex=b'foo...')  # (1)!

        assert 'foobar' == IsAnyStr(min_length=6)
        assert 'foobar' != IsAnyStr(min_length=8)

        assert 'foobar' == IsAnyStr(case='lower')
        assert 'Foobar' != IsAnyStr(case='lower')
        ```

        1. `regex` can be either a string or bytes, `IsAnyStr` will take care of conversion so checks work.
        """
        self.min_length = min_length
        self.max_length = max_length
        self.case = case
        self._flex = len(self.expected_types) > 1
        if regex is None:
            self.regex: Union[None, T, Pattern[T]] = None
            self.regex_flags: int = 0
        else:
            self.regex, self.regex_flags = self._prepare_regex(regex, regex_flags)
        super().__init__(
            min_length=Omit if min_length is None else min_length,
            max_length=Omit if max_length is None else max_length,
            case=case or Omit,
            regex=regex or Omit,
            regex_flags=Omit if regex_flags == 0 else plain_repr(repr(re.RegexFlag(regex_flags))),
        )

    def equals(self, other: Any) -> bool:
        if type(other) not in self.expected_types:
            return False

        if self.regex is not None:
            if self._flex and isinstance(other, str):
                other = other.encode()

            if not re.fullmatch(self.regex, other, flags=self.regex_flags):
                return False

        len_ = len(other)
        if self.min_length is not None and len_ < self.min_length:
            return False

        if self.max_length is not None and len_ > self.max_length:
            return False

        if self.case == 'upper' and not other.isupper():
            return False

        if self.case == 'lower' and not other.islower():
            return False

        return True

    def _prepare_regex(self, regex: Union[T, Pattern[T]], regex_flags: int) -> tuple[Union[T, Pattern[T]], int]:
        if isinstance(regex, re.Pattern):
            if self._flex:
                # less performant, but more flexible
                if regex_flags == 0 and regex.flags != re.UNICODE:
                    regex_flags = regex.flags & ~re.UNICODE
                regex = regex.pattern

            elif regex_flags != 0:
                regex = regex.pattern

        if self._flex and isinstance(regex, str):
            regex = regex.encode()  # type: ignore[assignment]

        return regex, regex_flags


class IsStr(IsAnyStr[str]):
    """
    Checks if the value is a string, and optionally meets some constraints.

    `IsStr` is a subclass of [`IsAnyStr`][dirty_equals.IsAnyStr] and therefore allows all the same arguments.

    Examples:
    ```py title="IsStr"
    from dirty_equals import IsStr

    assert 'foobar' == IsStr()
    assert b'foobar' != IsStr()
    assert 'foobar' == IsStr(regex='foo...')

    assert 'FOOBAR' == IsStr(min_length=5, max_length=10, case='upper')
    ```
    """

    expected_types = (str,)


class IsBytes(IsAnyStr[bytes]):
    """
    Checks if the value is a bytes object, and optionally meets some constraints.

    `IsBytes` is a subclass of [`IsAnyStr`][dirty_equals.IsAnyStr] and therefore allows all the same arguments.

    Examples:
    ```py title="IsBytes"
    from dirty_equals import IsBytes

    assert b'foobar' == IsBytes()
    assert 'foobar' != IsBytes()
    assert b'foobar' == IsBytes(regex=b'foo...')

    assert b'FOOBAR' == IsBytes(min_length=5, max_length=10, case='upper')
    ```
    """

    expected_types = (bytes,)
