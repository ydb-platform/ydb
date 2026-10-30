"""Utilities for encoding and decoding Temporal search attributes."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import temporalio.api.common.v1
import temporalio.common
from temporalio.converter._data_converter import default
from temporalio.converter._payload_converter import _get_iso_datetime_parser


def encode_search_attributes(
    attributes: (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ),
    api: temporalio.api.common.v1.SearchAttributes,
) -> None:
    """Convert search attributes into an API message.

    Args:
        attributes: Search attributes to convert. The dictionary form of this is
            DEPRECATED.
        api: API message to set converted attributes on.
    """
    if isinstance(attributes, temporalio.common.TypedSearchAttributes):
        for typed_k, typed_v in attributes:
            api.indexed_fields[typed_k.name].CopyFrom(
                encode_typed_search_attribute_value(typed_k, typed_v)
            )
        return
    elif not attributes:
        return
    for k, v in attributes.items():
        api.indexed_fields[k].CopyFrom(encode_search_attribute_values(v))


def encode_typed_search_attribute_value(
    key: temporalio.common.SearchAttributeKey[
        temporalio.common.SearchAttributeValueType
    ],
    value: temporalio.common.SearchAttributeValue | None,
) -> temporalio.api.common.v1.Payload:
    """Convert typed search attribute value into a payload.

    Args:
        key: Key for the value.
        value: Value to convert.

    Returns:
        Payload for the value.
    """
    # For server search attributes to work properly, we cannot set the metadata
    # type when we set null
    if value is None:
        return default().payload_converter.to_payload(None)
    if not isinstance(value, key.origin_value_type):
        raise TypeError(
            f"Value of type {value} not suitable for indexed value type {key.indexed_value_type}"
        )
    # datetime needs to be in isoformat
    if isinstance(value, datetime):
        value = value.isoformat()
    # We'll do an extra sanity check for keyword list and check every value
    if isinstance(value, Sequence):
        for v in value:
            if not isinstance(v, str):
                raise TypeError("All values of a keyword list must be strings")
    # Convert value
    payload = default().payload_converter.to_payload(value)
    # Set metadata type
    payload.metadata["type"] = key._metadata_type.encode()
    return payload


def encode_search_attribute_values(
    vals: temporalio.common.SearchAttributeValues,
) -> temporalio.api.common.v1.Payload:
    """Convert search attribute values into a payload.

    .. deprecated::
        Use typed search attributes instead.

    Args:
        vals: List of values to convert.
    """
    if not isinstance(vals, list):
        raise TypeError("Search attribute values must be lists")  # type:ignore[reportUnreachable]
    # Confirm all types are the same
    val_type: type | None = None
    # Convert dates to strings
    safe_vals = []
    for v in vals:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                raise ValueError(
                    "Timezone must be present on all search attribute dates"
                )
            v = v.isoformat()
        elif not isinstance(v, (str, int, float, bool)):
            raise TypeError(
                f"Search attribute value of type {type(v).__name__} not one of str, int, float, bool, or datetime"
            )
        elif val_type and type(v) is not val_type:
            raise TypeError(
                "Search attribute values must have the same type for the same key"
            )
        elif not val_type:
            val_type = type(v)
        safe_vals.append(v)
    return default().payload_converter.to_payloads([safe_vals])[0]


def _encode_maybe_typed_search_attributes(  # type:ignore[reportUnusedFunction]
    non_typed_attributes: temporalio.common.SearchAttributes | None,
    typed_attributes: temporalio.common.TypedSearchAttributes | None,
    api: temporalio.api.common.v1.SearchAttributes,
) -> None:
    if non_typed_attributes:
        if typed_attributes and typed_attributes.search_attributes:
            raise ValueError(
                "Cannot provide both deprecated search attributes and typed search attributes"
            )
        encode_search_attributes(non_typed_attributes, api)
    elif typed_attributes and typed_attributes.search_attributes:
        encode_search_attributes(typed_attributes, api)


def decode_search_attributes(
    api: temporalio.api.common.v1.SearchAttributes,
) -> temporalio.common.SearchAttributes:
    """Decode API search attributes to values.

    .. deprecated::
        Use typed search attributes instead.

    Args:
        api: API message with search attribute values to convert.

    Returns:
        Converted search attribute values (new mapping every time).
    """
    conv = default().payload_converter
    ret = {}
    for k, v in api.indexed_fields.items():
        val = conv.from_payloads([v])[0]
        # If a value did not come back as a list, make it a single-item list
        if not isinstance(val, list):
            val = [val]
        # Convert each item to datetime if necessary
        if v.metadata.get("type") == b"Datetime":
            parser = _get_iso_datetime_parser()
            val = [parser(v) for v in val]
        ret[k] = val
    return ret


def decode_typed_search_attributes(
    api: temporalio.api.common.v1.SearchAttributes,
) -> temporalio.common.TypedSearchAttributes:
    """Decode API search attributes to typed search attributes.

    Args:
        api: API message with search attribute values to convert.

    Returns:
        Typed search attribute collection (new object every time).
    """
    conv = default().payload_converter
    pairs: list[temporalio.common.SearchAttributePair] = []
    for k, v in api.indexed_fields.items():
        # We want the "type" metadata, but if it is not present or an unknown
        # type, we will just ignore
        metadata_type = v.metadata.get("type")
        if not metadata_type:
            continue
        key = temporalio.common.SearchAttributeKey._from_metadata_type(
            k, metadata_type.decode()
        )
        if not key:
            continue
        val = conv.from_payload(v)
        # If the value is a list but the type is not keyword list, pull out
        # single item or consider this an invalid value and ignore
        if (
            key.indexed_value_type
            != temporalio.common.SearchAttributeIndexedValueType.KEYWORD_LIST
            and isinstance(val, list)
        ):
            if len(val) != 1:
                continue
            val = val[0]
        if (
            key.indexed_value_type
            == temporalio.common.SearchAttributeIndexedValueType.DATETIME
        ):
            parser = _get_iso_datetime_parser()
            # We will let this throw
            val = parser(val)
        # If the value isn't the right type, we need to ignore
        if isinstance(val, key.origin_value_type):
            pairs.append(temporalio.common.SearchAttributePair(key, val))
    return temporalio.common.TypedSearchAttributes(pairs)


def _decode_search_attribute_value(  # type:ignore[reportUnusedFunction]
    payload: temporalio.api.common.v1.Payload,
) -> temporalio.common.SearchAttributeValue:
    val = default().payload_converter.from_payload(payload)
    if isinstance(val, str) and payload.metadata.get("type") == b"Datetime":
        val = _get_iso_datetime_parser()(val)
    return val  # type: ignore
