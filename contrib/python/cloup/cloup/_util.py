"""Generic utilities."""
from typing import (
    Any, Dict, Hashable, Iterable, List, Optional, Sequence, Type, TypeVar,
)

import click

from cloup.typing import MISSING, Possibly

click_version_tuple = tuple(click.__version__.split('.'))
click_major = int(click_version_tuple[0])
click_minor = int(click_version_tuple[1])
click_version_ge_8_1 = (click_major, click_minor) >= (8, 1)
click_version_ge_8_2 = (click_major, click_minor) >= (8, 2)

T = TypeVar('T')
K = TypeVar('K', bound=Hashable)
V = TypeVar('V')


def pick_non_missing(d: Dict[K, Possibly[V]]) -> Dict[K, V]:
    return {key: val for key, val in d.items() if val is not MISSING}


def class_name(obj: object) -> str:
    return obj.__class__.__name__


def check_arg(condition: bool, msg: str = '') -> None:
    if not condition:
        raise ValueError(msg)


def indent_lines(lines: Iterable[str], width: int = 2) -> List[str]:
    spaces = ' ' * width
    return [spaces + line for line in lines]


def make_repr(
    obj: Any,
    *args: Any,
    _line_len: int = 60,
    _indent: int = 2,
    **kwargs: Any
) -> str:
    """
    Generate repr(obj).

    :param obj:
        object to represent
    :param args:
        positional arguments in the repr
    :param _line_len:
        if the repr length exceeds this, arguments will be on their own line;
        if negative, the repr will be in a single line regardless of its length
    :param _indent:
        indentation width of arguments in case they are shown in their own line
    :param kwargs:
        keyword arguments in the repr
    :return: str
    """
    cls_name = obj.__class__.__name__
    arglist = [
        *(repr(arg) for arg in args),
        *(f'{key}={value!r}' for key, value in kwargs.items()),
    ]
    len_arglist = sum(len(s) for s in arglist)
    total_len = len(cls_name) + len_arglist + 2 * len(arglist)
    if 0 <= _line_len < total_len:
        lines = indent_lines(arglist, width=_indent)
        args_text = ',\n'.join(lines)
        return f'{cls_name}(\n{args_text}\n)'
    else:
        args_text = ', '.join(arglist)
        return f'{cls_name}({args_text})'


def make_one_line_repr(obj: object, *args: Any, **kwargs: Any) -> str:
    return make_repr(obj, *args, _line_len=-1, **kwargs)


def pluralize(
    count: int, zero: str = '', one: str = '', many: str = '',
) -> str:
    if count == 0 and zero:
        return zero
    if count == 1 and one:
        return one
    return many.format(count=count)


def coalesce(*values: Optional[T]) -> Optional[T]:
    """Return the first value that is not ``None``
    (or ``None`` if no such value exists)."""
    return next((val for val in values if val is not None), None)


def first_bool(*values: Any) -> bool:
    """Return the first bool (or raises ``StopIteration`` if no bool is found)."""
    return next(val for val in values if isinstance(val, bool))


def pick_not_none(iterable: Iterable[Optional[T]]) -> List[T]:
    return [x for x in iterable if x is not None]


def check_positive_int(value: Any, arg_name: str) -> None:
    error_type: Optional[Type[Exception]] = None
    if not isinstance(value, int):
        error_type = TypeError
    elif value <= 0:
        error_type = ValueError
    if error_type:
        raise error_type(
            f'argument `{arg_name}` should be a positive integer; it is {value!r}'
        )


def identity(x: T) -> T:
    return x


class FrozenSpaceMeta(type):
    def __init__(cls, *args: Any):
        super().__init__(*args)
        d = {k: v for k, v in vars(cls).items() if not k.startswith('_')}
        type.__setattr__(cls, '_dict', d)

    def __setattr__(cls, key: str, value: Any) -> None:
        if key.startswith("__"):
            return super().__setattr__(key, value)
        else:
            raise Exception(
                "you can't set attributes on this class; only special dunder attributes "
                "(e.g. __annotations__) are allowed to be set for compatibility reasons."
            )

    def asdict(cls) -> Dict[str, Any]:
        return cls._dict  # type: ignore

    def __contains__(cls, item: str) -> bool:
        return item in cls.asdict()

    def __getitem__(cls, item: str) -> Any:
        return cls._dict[item]  # type: ignore


class FrozenSpace(metaclass=FrozenSpaceMeta):
    """A class used just as frozen namespace for constants."""

    def __init__(self) -> None:
        raise Exception(
            "this class is just a namespace for constants, it's not instantiable.")


def delete_keys(d: Dict[Any, Any], keys: Sequence[str]) -> None:
    for key in keys:
        del d[key]


def reindent(text: str, indent: int = 0) -> str:
    import textwrap as tw
    if text.startswith('\n'):
        text = text[1:]
    text = tw.dedent(text)
    if indent:
        return tw.indent(text, ' ' * indent)
    return text
