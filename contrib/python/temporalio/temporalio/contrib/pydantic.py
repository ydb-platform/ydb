"""A data converter for Pydantic v2.

To use, pass ``pydantic_data_converter`` as the ``data_converter`` argument to
:py:class:`temporalio.client.Client`:

.. code-block:: python

    client = Client(
        data_converter=pydantic_data_converter,
        ...
    )

Pydantic v1 is not supported.
"""

import functools
from dataclasses import dataclass
from typing import Any

from pydantic import TypeAdapter
from pydantic_core import SchemaSerializer, to_json
from pydantic_core.core_schema import any_schema

import temporalio.api.common.v1
from temporalio.converter import (
    CompositePayloadConverter,
    DataConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter,
    JSONPlainPayloadConverter,
)

# Note that in addition to the implementation in this module, _RestrictedProxy
# implements __get_pydantic_core_schema__ so that pydantic unwraps proxied types.


@dataclass
class ToJsonOptions:
    """Options for converting to JSON with pydantic."""

    exclude_unset: bool = False


class PydanticJSONPlainPayloadConverter(EncodingPayloadConverter):
    """Pydantic JSON payload converter.

    Supports conversion of all types supported by Pydantic to and from JSON.

    In addition to Pydantic models, these include all `json.dump`-able types,
    various non-`json.dump`-able standard library types such as dataclasses,
    types from the datetime module, sets, UUID, etc, and custom types composed
    of any of these.

    See https://docs.pydantic.dev/latest/api/standard_library_types/
    """

    def __init__(
        self,
        to_json_options: ToJsonOptions | None = None,
        *,
        max_cached_type_adapters: int | None = 1024,
    ) -> None:
        """Create a new payload converter.

        Args:
            to_json_options: Options for serializing values to JSON.
            max_cached_type_adapters: Maximum number of type adapters to
                cache, with least-recently-used eviction. Defaults to 1024.
                If ``None``, the cache is unbounded. If zero, caching is
                disabled.
        """
        if max_cached_type_adapters is not None and max_cached_type_adapters < 0:
            raise ValueError("max_cached_type_adapters cannot be negative")
        self._schema_serializer = SchemaSerializer(any_schema())
        self._to_json_options = to_json_options
        self._type_adapter = functools.lru_cache(maxsize=max_cached_type_adapters)(
            TypeAdapter
        )

    @property
    def encoding(self) -> str:
        """See base class."""
        return "json/plain"

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        """See base class.

        Uses ``pydantic_core.to_json`` to serialize ``value`` to JSON.

        See
        https://docs.pydantic.dev/latest/api/pydantic_core/#pydantic_core.to_json.
        """
        data = (
            self._schema_serializer.to_json(
                value, exclude_unset=self._to_json_options.exclude_unset
            )
            if self._to_json_options
            else to_json(value)
        )
        return temporalio.api.common.v1.Payload(
            metadata={"encoding": self.encoding.encode()}, data=data
        )

    def from_payload(
        self,
        payload: temporalio.api.common.v1.Payload,
        type_hint: type | None = None,
    ) -> Any:
        """See base class.

        Uses ``pydantic.TypeAdapter.validate_json`` to construct an
        instance of the type specified by ``type_hint`` from the JSON payload.
        Type adapters are cached per hashable type hint; see
        ``max_cached_type_adapters`` on the constructor.

        See
        https://docs.pydantic.dev/latest/api/type_adapter/#pydantic.type_adapter.TypeAdapter.validate_json.
        """
        _type_hint = type_hint if type_hint is not None else Any
        type_adapter: TypeAdapter[Any]
        try:
            type_adapter = self._type_adapter(_type_hint)
        except TypeError:
            # Distinguish an unhashable hint (bypass the cache) from a
            # TypeError raised while constructing the adapter (re-raise).
            try:
                hash(_type_hint)
            except TypeError:
                type_adapter = TypeAdapter(_type_hint)
            else:
                raise
        return type_adapter.validate_json(payload.data)


class PydanticPayloadConverter(CompositePayloadConverter):
    """Payload converter for payloads containing pydantic model instances.

    JSON conversion is replaced with a converter that uses
    :py:class:`PydanticJSONPlainPayloadConverter`.
    """

    def __init__(
        self,
        to_json_options: ToJsonOptions | None = None,
        *,
        max_cached_type_adapters: int | None = 1024,
    ) -> None:
        """Initialize object.

        Args:
            to_json_options: Options for serializing values to JSON.
            max_cached_type_adapters: Maximum number of type adapters to
                cache, with least-recently-used eviction. Defaults to 1024.
                If ``None``, the cache is unbounded. If zero, caching is
                disabled.

                To configure this through a :py:class:`DataConverter`, use a
                nullary subclass as the payload converter class::

                    class MyPayloadConverter(PydanticPayloadConverter):
                        def __init__(self) -> None:
                            super().__init__(max_cached_type_adapters=128)

                    my_data_converter = DataConverter(
                        payload_converter_class=MyPayloadConverter
                    )
        """
        json_payload_converter = PydanticJSONPlainPayloadConverter(
            to_json_options,
            max_cached_type_adapters=max_cached_type_adapters,
        )
        super().__init__(
            *(
                c
                if not isinstance(c, JSONPlainPayloadConverter)
                else json_payload_converter
                for c in DefaultPayloadConverter.default_encoding_payload_converters
            )
        )


pydantic_data_converter = DataConverter(
    payload_converter_class=PydanticPayloadConverter
)
"""Pydantic data converter.

Supports conversion of all types supported by Pydantic to and from JSON.

In addition to Pydantic models, these include all `json.dump`-able types,
various non-`json.dump`-able standard library types such as dataclasses,
types from the datetime module, sets, UUID, etc, and custom types composed
of any of these.

To use, pass as the ``data_converter`` argument of :py:class:`temporalio.client.Client`
"""
