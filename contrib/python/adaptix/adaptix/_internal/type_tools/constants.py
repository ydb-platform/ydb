# mypy: disable-error-code="dict-item"
import collections
import concurrent.futures
import queue
import re
from collections.abc import Mapping
from os import PathLike
from typing import TypeVar

from ..common import VarTuple

_AnyStrT = TypeVar("_AnyStrT", str, bytes)
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T1_co = TypeVar("_T1_co", covariant=True)
_AnyStr_co = TypeVar("_AnyStr_co", str, bytes, covariant=True)

BUILTIN_ORIGIN_TO_TYPEVARS: Mapping[type, VarTuple[TypeVar]] = {
    re.Pattern: (_AnyStrT, ),
    re.Match: (_AnyStrT, ),
    PathLike: (_AnyStr_co, ),
    type: (_T1,),
    list: (_T1,),
    set: (_T1,),
    frozenset: (_T1_co, ),
    collections.Counter: (_T1,),
    collections.deque: (_T1,),
    dict: (_T1, _T2),
    collections.defaultdict: (_T1, _T2),
    collections.OrderedDict: (_T1, _T2),
    collections.ChainMap: (_T1, _T2),
    queue.Queue: (_T1, ),
    queue.PriorityQueue: (_T1, ),
    queue.LifoQueue: (_T1, ),
    queue.SimpleQueue: (_T1, ),
    concurrent.futures.Future: (_T1, ),
}
