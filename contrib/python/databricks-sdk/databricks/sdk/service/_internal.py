import datetime
import urllib.parse
from typing import Callable, Dict, Generic, List, Optional, Type, TypeVar

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from databricks.sdk.common.types.fieldmask import FieldMask


def _from_dict(d: Dict[str, any], field: str, cls: Type) -> any:
    if field not in d or d[field] is None:
        return None
    return getattr(cls, "from_dict")(d[field])


def _repeated_dict(d: Dict[str, any], field: str, cls: Type) -> any:
    if field not in d or not d[field]:
        return []
    from_dict = getattr(cls, "from_dict")
    return [from_dict(v) for v in d[field]]


def _get_enum_value(cls: Type, value: str) -> Optional[Type]:
    return next(
        (v for v in getattr(cls, "__members__").values() if v.value == value),
        None,
    )


def _enum(d: Dict[str, any], field: str, cls: Type) -> any:
    """Unknown enum values are returned as None."""
    if field not in d or not d[field]:
        return None
    return _get_enum_value(cls, d[field])


def _repeated_enum(d: Dict[str, any], field: str, cls: Type) -> any:
    """For now, unknown enum values are not included in the response."""
    if field not in d or not d[field]:
        return None
    res = []
    for e in d[field]:
        val = _get_enum_value(cls, e)
        if val:
            res.append(val)
    return res


def _escape_multi_segment_path_parameter(param: str) -> str:
    return urllib.parse.quote(param)


def _timestamp(d: Dict[str, any], field: str) -> Optional[Timestamp]:
    """
    Helper function to convert a timestamp string to a Timestamp object.
    It takes a dictionary and a field name, and returns a Timestamp object.
    The field name is the key in the dictionary that contains the timestamp string.
    """
    if field not in d or not d[field]:
        return None
    ts = Timestamp()
    ts.FromJsonString(d[field])
    return ts


def _repeated_timestamp(d: Dict[str, any], field: str) -> Optional[List[Timestamp]]:
    """
    Helper function to convert a list of timestamp strings to a list of Timestamp objects.
    It takes a dictionary and a field name, and returns a list of Timestamp objects.
    The field name is the key in the dictionary that contains the list of timestamp strings.
    """
    if field not in d or not d[field]:
        return None
    result = []
    for v in d[field]:
        ts = Timestamp()
        ts.FromJsonString(v)
        result.append(ts)
    return result


def _duration(d: Dict[str, any], field: str) -> Optional[Duration]:
    """
    Helper function to convert a duration string to a Duration object.
    It takes a dictionary and a field name, and returns a Duration object.
    The field name is the key in the dictionary that contains the duration string.
    """
    if field not in d or not d[field]:
        return None
    dur = Duration()
    dur.FromJsonString(d[field])
    return dur


def _repeated_duration(d: Dict[str, any], field: str) -> Optional[List[Duration]]:
    """
    Helper function to convert a list of duration strings to a list of Duration objects.
    It takes a dictionary and a field name, and returns a list of Duration objects.
    The field name is the key in the dictionary that contains the list of duration strings.
    """
    if field not in d or not d[field]:
        return None
    result = []
    for v in d[field]:
        dur = Duration()
        dur.FromJsonString(v)
        result.append(dur)
    return result


def _fieldmask(d: Dict[str, any], field: str) -> Optional[FieldMask]:
    """
    Helper function to convert a fieldmask string to a FieldMask object.
    It takes a dictionary and a field name, and returns a FieldMask object.
    The field name is the key in the dictionary that contains the fieldmask string.
    """
    if field not in d or not d[field]:
        return None
    fm = FieldMask()
    fm.FromJsonString(d[field])
    return fm


def _repeated_fieldmask(d: Dict[str, any], field: str) -> Optional[List[FieldMask]]:
    """
    Helper function to convert a list of fieldmask strings to a list of FieldMask objects.
    It takes a dictionary and a field name, and returns a list of FieldMask objects.
    The field name is the key in the dictionary that contains the list of fieldmask strings.
    """
    if field not in d or not d[field]:
        return None
    result = []
    for v in d[field]:
        fm = FieldMask()
        fm.FromJsonString(v)
        result.append(fm)
    return result


ReturnType = TypeVar("ReturnType")


class Wait(Generic[ReturnType]):

    def __init__(self, waiter: Callable, response: any = None, **kwargs) -> None:
        self.response = response

        self._waiter = waiter
        self._bind = kwargs

    def __getattr__(self, key) -> any:
        return self._bind[key]

    def bind(self) -> dict:
        return self._bind

    def result(
        self,
        timeout: datetime.timedelta = datetime.timedelta(minutes=20),
        callback: Callable[[ReturnType], None] = None,
    ) -> ReturnType:
        kwargs = self._bind.copy()
        return self._waiter(callback=callback, timeout=timeout, **kwargs)
