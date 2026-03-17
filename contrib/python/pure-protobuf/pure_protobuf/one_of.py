from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any, Callable, Generic, Optional, TypeVar

from pure_protobuf.message import BaseMessage

OneOfT = TypeVar("OneOfT")


class OneOf(Generic[OneOfT]):
    """
    See Also:
        - https://developers.google.com/protocol-buffers/docs/proto3#oneof.
    """

    __slots__ = ("_fields",)

    def __init__(self) -> None:
        """
        Define a one-of group of fields.

        A [`Field`][pure_protobuf.annotations.Field] then should be assigned to the group
        via the [`one_of`][pure_protobuf.annotations.Field.one_of] parameter.
        """
        self._fields: list[tuple[int, str]] = []

    def _add_field(self, number: int, name: str) -> None:
        self._fields.append((number, name))

    def _keep_values(
        self,
        values: MutableMapping[str, Any],
        keep_number: int,
    ) -> None:
        for other_number, other_name in self._fields:
            if other_number != keep_number:
                values.pop(other_name, None)

    def _keep_attribute(self, message: BaseMessage, keep_number: int) -> None:
        for other_number, other_name in self._fields:
            if other_number != keep_number:
                super(BaseMessage, message).__setattr__(other_name, None)

    def __get__(self, instance: Any, type_: type[Any]) -> Optional[OneOfT]:  # noqa: D105
        if not isinstance(instance, BaseMessage):
            # Allows passing the descriptor by reference, and we need to move the descriptor from
            # the corresponding annotation.
            # This is not a part of the public interface, hence the «type: ignore».
            return self  # type: ignore[return-value]
        name = self._which_one_of(instance)
        return getattr(instance, name) if name else None

    def __set__(self, instance: BaseMessage, _value: Any) -> None:  # noqa: D105
        raise RuntimeError("attempted to set the one-of field, use a specific attribute instead")

    def which_one_of_getter(self) -> Callable[[BaseMessage], Optional[str]]:
        """Construct a getter which returns which attribute is actually set."""

        def which_one_of(instance: BaseMessage) -> Optional[str]:
            return self._which_one_of(instance)

        return which_one_of

    def _which_one_of(self, instance: BaseMessage) -> Optional[str]:
        """Return which of the attributes is actually set."""
        for _, name in self._fields:
            value = getattr(instance, name)
            if value is not None:
                return name
        return None
