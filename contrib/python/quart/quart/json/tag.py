from __future__ import annotations

from base64 import b64decode, b64encode
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Type
from uuid import UUID

from markupsafe import Markup
from werkzeug.http import parse_date

from quart.json import dumps, loads


class JSONTag:
    key: Optional[str] = None

    def __init__(self, serializer: "TaggedJSONSerializer") -> None:
        self.serializer = serializer

    def check(self, value: Any) -> bool:
        raise NotImplementedError

    def to_json(self, value: Any) -> Any:
        raise NotImplementedError

    def to_python(self, value: str) -> Any:
        raise NotImplementedError

    def tag(self, value: Any) -> Any:
        return {self.key: self.to_json(value)}


class TagDict(JSONTag):
    key = " di"

    def check(self, value: Any) -> bool:
        return (
            isinstance(value, dict)
            and len(value) == 1
            and next(iter(value)) in self.serializer.tags
        )

    def to_json(self, value: Any) -> Dict[str, Any]:
        key = next(iter(value))
        return {key + "__": self.serializer.tag(value[key])}

    def to_python(self, value: str) -> Dict[str, Any]:
        key, item = next(iter(value))  # type: ignore
        return {key[:-2]: item}  # type: ignore


class PassDict(JSONTag):
    def check(self, value: Any) -> bool:
        return isinstance(value, dict)

    def to_json(self, value: Any) -> Dict[str, Any]:
        return {key: self.serializer.tag(item) for key, item in value.items()}

    tag = to_json


class TagTuple(JSONTag):
    key = " t"

    def check(self, value: Any) -> bool:
        return isinstance(value, tuple)

    def to_json(self, value: Tuple[Any]) -> List[Any]:
        return [self.serializer.tag(item) for item in value]

    def to_python(self, value: Any) -> Tuple[Any, ...]:
        return tuple(value)


class PassList(JSONTag):
    def check(self, value: Any) -> bool:
        return isinstance(value, list)

    def to_json(self, value: List[Any]) -> List[Any]:
        return [self.serializer.tag(item) for item in value]

    tag = to_json


class TagBytes(JSONTag):
    key = " b"

    def check(self, value: Any) -> bool:
        return isinstance(value, bytes)

    def to_json(self, value: bytes) -> str:
        return b64encode(value).decode("ascii")

    def to_python(self, value: str) -> bytes:
        return b64decode(value)


class TagMarkup(JSONTag):
    key = " m"

    def check(self, value: Any) -> bool:
        return callable(getattr(value, "__html__", None))

    def to_json(self, value: Any) -> str:
        return str(value.__html__())

    def to_python(self, value: str) -> Markup:
        return Markup(value)


class TagUUID(JSONTag):
    key = " u"

    def check(self, value: Any) -> bool:
        return isinstance(value, UUID)

    def to_json(self, value: Any) -> str:
        return value.hex

    def to_python(self, value: str) -> UUID:
        return UUID(value)


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return parse_date(value)


class TagDateTime(JSONTag):
    key = " d"

    def check(self, value: Any) -> bool:
        return isinstance(value, datetime)

    def to_json(self, value: datetime) -> str:
        return value.isoformat(timespec="microseconds")

    def to_python(self, value: str) -> datetime:
        return _parse_datetime(value)


class TaggedJSONSerializer:

    default_tags = [
        TagDict,
        PassDict,
        TagTuple,
        PassList,
        TagBytes,
        TagMarkup,
        TagUUID,
        TagDateTime,
    ]

    def __init__(self) -> None:
        self.tags: Dict[str, JSONTag] = {}
        self.order: List[JSONTag] = []

        for tag_class in self.default_tags:
            self.register(tag_class)

    def register(
        self, tag_class: Type[JSONTag], force: bool = False, index: Optional[int] = None
    ) -> None:
        tag = tag_class(self)
        key = tag.key

        if key is not None:
            if not force and key in self.tags:
                raise KeyError(f"Tag '{key}' is already registered.")

            self.tags[key] = tag

        if index is None:
            self.order.append(tag)
        else:
            self.order.insert(index, tag)

    def tag(self, value: Any) -> Dict[str, Any]:
        for tag in self.order:
            if tag.check(value):
                return tag.tag(value)

        return value

    def untag(self, value: Dict[str, Any]) -> Any:
        if len(value) != 1:
            return value

        key = next(iter(value))

        if key not in self.tags:
            return value

        return self.tags[key].to_python(value[key])

    def dumps(self, value: Any) -> str:
        return dumps(self.tag(value), separators=(",", ":"))

    def loads(self, value: str) -> Any:
        return loads(value, object_hook=self.untag)
