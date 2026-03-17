import inspect
import sys
from enum import Enum
from typing import TYPE_CHECKING, Annotated, ForwardRef, TypeVar


def _get_caller_module(stack_offset: int):
    return sys.modules[inspect.stack()[stack_offset].frame.f_globals["__name__"]]


class FwdRefMarker(Enum):
    VALUE = "VALUE"


if TYPE_CHECKING:
    T = TypeVar("T")
    FwdRef = Annotated[T, FwdRefMarker.VALUE]
else:
    class FwdRef:
        def __class_getitem__(cls, item):
            return Annotated[ForwardRef(item, module=_get_caller_module(2)), FwdRefMarker.VALUE]
