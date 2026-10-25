# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from opentelemetry.context import Context
from opentelemetry.util.types import Attributes


class Observation:
    """A measurement observed in an asynchronous instrument

    Return/yield instances of this class from asynchronous instrument callbacks.

    Args:
        value: The float or int measured value
        attributes: The measurement's attributes
        context: The measurement's context
    """

    def __init__(
        self,
        value: int | float,
        attributes: Attributes = None,
        context: Context | None = None,
    ) -> None:
        self._value = value
        self._attributes = attributes
        self._context = context

    @property
    def value(self) -> float | int:
        return self._value

    @property
    def attributes(self) -> Attributes:
        return self._attributes

    @property
    def context(self) -> Context | None:
        return self._context

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Observation)
            and self.value == other.value
            and self.attributes == other.attributes
            and self.context == other.context
        )

    def __repr__(self) -> str:
        return f"Observation(value={self.value}, attributes={self.attributes}, context={self.context})"
