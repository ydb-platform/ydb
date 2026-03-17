# -*- coding: utf-8 -*-
import typing
from mypy_extensions import Arg

try:
    from typing import NoReturn
except ImportError:
    NoReturn = None  # type: ignore


Func = typing.Callable[..., typing.Any]
FuncType = typing.TypeVar('FuncType', bound=Func)
JSONDict = typing.Dict[typing.Text, typing.Any]
MarshalingMethod = typing.Callable[[Arg(typing.Any, 'value')], typing.Any]
UnmarshalingMethod = typing.Callable[[Arg(typing.Any, 'value')], typing.Any]
