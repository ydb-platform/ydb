from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Self

    from zarr.core.common import JSON

from dataclasses import dataclass, fields

__all__ = ["Metadata"]


@dataclass(frozen=True)
class Metadata:
    def to_dict(self) -> dict[str, JSON]:
        """
        Recursively serialize this model to a dictionary.
        This method inspects the fields of self and calls `x.to_dict()` for any fields that
        are instances of `Metadata`. Sequences of `Metadata` are similarly recursed into, and
        the output of that recursion is collected in a list.
        """
        out_dict = {}
        for field in fields(self):
            key = field.name
            value = getattr(self, key)
            if isinstance(value, Metadata):
                out_dict[field.name] = getattr(self, field.name).to_dict()
            elif isinstance(value, str):
                out_dict[key] = value
            elif isinstance(value, Sequence):
                out_dict[key] = tuple(v.to_dict() if isinstance(v, Metadata) else v for v in value)
            else:
                out_dict[key] = value

        return out_dict

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        """
        Create an instance of the model from a dictionary
        """

        return cls(**data)
