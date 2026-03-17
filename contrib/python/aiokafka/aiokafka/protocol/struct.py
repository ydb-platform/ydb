from io import BytesIO
from typing import Any, ClassVar

from typing_extensions import Self

from .types import Schema


class Struct:
    SCHEMA: ClassVar = Schema()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if len(args) == len(self.SCHEMA.fields):
            for i, name in enumerate(self.SCHEMA.names):
                self.__dict__[name] = args[i]
        elif len(args) > 0:
            raise ValueError("Args must be empty or mirror schema")
        else:
            for name in self.SCHEMA.names:
                self.__dict__[name] = kwargs.pop(name, None)
            if kwargs:
                raise ValueError(
                    "Keyword(s) not in schema {}: {}".format(
                        list(self.SCHEMA.names), ", ".join(kwargs.keys())
                    )
                )

    def encode(self) -> bytes:
        return self.SCHEMA.encode([self.__dict__[name] for name in self.SCHEMA.names])

    @classmethod
    def decode(cls, data: BytesIO | bytes) -> Self:
        if isinstance(data, bytes):
            data = BytesIO(data)
        return cls(*[field.decode(data) for field in cls.SCHEMA.fields])

    def get_item(self, name: str) -> Any:
        if name not in self.SCHEMA.names:
            raise KeyError(f"{name} is not in the schema")
        return self.__dict__[name]

    def __repr__(self) -> str:
        key_vals: list[str] = []
        for name, field in zip(self.SCHEMA.names, self.SCHEMA.fields, strict=False):
            key_vals.append(f"{name}={field.repr(self.__dict__[name])}")
        return self.__class__.__name__ + "(" + ", ".join(key_vals) + ")"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Struct):
            return NotImplemented
        if self.SCHEMA != other.SCHEMA:
            return False
        for attr in self.SCHEMA.names:
            if self.__dict__[attr] != other.__dict__[attr]:
                return False
        return True

    __hash__ = object.__hash__  # unhashable
