#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import base64
import collections.abc
import ipaddress
from copy import deepcopy
from datetime import date, datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

from dateutil import parser, tz
from elastic_transport.client_utils import DEFAULT, DefaultType

from .exceptions import ValidationException
from .query import Q
from .utils import AttrDict, AttrList, DslBase
from .wrappers import Range

if TYPE_CHECKING:
    from datetime import tzinfo
    from ipaddress import IPv4Address, IPv6Address

    from _operator import _SupportsComparison

    from . import types
    from .document import InnerDoc
    from .document_base import InstrumentedField
    from .mapping_base import MappingBase
    from .query import Query

unicode = str


def construct_field(
    name_or_field: Union[
        str,
        "Field",
        Dict[str, Any],
    ],
    **params: Any,
) -> "Field":
    # {"type": "text", "analyzer": "snowball"}
    if isinstance(name_or_field, collections.abc.Mapping):
        if params:
            raise ValueError(
                "construct_field() cannot accept parameters when passing in a dict."
            )
        params = deepcopy(name_or_field)
        if "type" not in params:
            # inner object can be implicitly defined
            if "properties" in params:
                name = "object"
            else:
                raise ValueError('construct_field() needs to have a "type" key.')
        else:
            name = params.pop("type")
        return Field.get_dsl_class(name)(**params)

    # Text()
    if isinstance(name_or_field, Field):
        if params:
            raise ValueError(
                "construct_field() cannot accept parameters "
                "when passing in a construct_field object."
            )
        return name_or_field

    # "text", analyzer="snowball"
    return Field.get_dsl_class(name_or_field)(**params)


class Field(DslBase):
    _type_name = "field"
    _type_shortcut = staticmethod(construct_field)
    # all fields can be multifields
    _param_defs = {"fields": {"type": "field", "hash": True}}
    name = ""
    _coerce = False

    def __init__(
        self, multi: bool = False, required: bool = False, *args: Any, **kwargs: Any
    ):
        """
        :arg bool multi: specifies whether field can contain array of values
        :arg bool required: specifies whether field is required
        """
        self._multi = multi
        self._required = required
        super().__init__(*args, **kwargs)

    def __getitem__(self, subfield: str) -> "Field":
        return cast(Field, self._params.get("fields", {})[subfield])

    def _serialize(self, data: Any, skip_empty: bool) -> Any:
        return data

    def _safe_serialize(self, data: Any, skip_empty: bool) -> Any:
        try:
            return self._serialize(data, skip_empty)
        except TypeError:
            # older method signature, without skip_empty
            return self._serialize(data)  # type: ignore[call-arg]

    def _deserialize(self, data: Any) -> Any:
        return data

    def _empty(self) -> Optional[Any]:
        return None

    def empty(self) -> Optional[Any]:
        if self._multi:
            return AttrList([])
        return self._empty()

    def serialize(self, data: Any, skip_empty: bool = True) -> Any:
        if isinstance(data, (list, AttrList, tuple)):
            return list(
                map(
                    self._safe_serialize,
                    cast(Iterable[Any], data),
                    [skip_empty] * len(data),
                )
            )
        return self._safe_serialize(data, skip_empty)

    def deserialize(self, data: Any) -> Any:
        if isinstance(data, (list, AttrList, tuple)):
            data = [
                None if d is None else self._deserialize(d)
                for d in cast(Iterable[Any], data)
            ]
            return data
        if data is None:
            return None
        return self._deserialize(data)

    def clean(self, data: Any) -> Any:
        if data is not None:
            data = self.deserialize(data)
        if data in (None, [], {}) and self._required:
            raise ValidationException("Value required for this field.")
        return data

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        name, value = cast(Tuple[str, Dict[str, Any]], d.popitem())
        value["type"] = name
        return value


class CustomField(Field):
    name = "custom"
    _coerce = True

    def to_dict(self) -> Dict[str, Any]:
        if isinstance(self.builtin_type, Field):
            return self.builtin_type.to_dict()

        d = super().to_dict()
        d["type"] = self.builtin_type
        return d


class RangeField(Field):
    _coerce = True
    _core_field: Optional[Field] = None

    def _deserialize(self, data: Any) -> Range["_SupportsComparison"]:
        if isinstance(data, Range):
            return data
        data = {k: self._core_field.deserialize(v) for k, v in data.items()}  # type: ignore[union-attr]
        return Range(data)

    def _serialize(self, data: Any, skip_empty: bool) -> Optional[Dict[str, Any]]:
        if data is None:
            return None
        if not isinstance(data, collections.abc.Mapping):
            data = data.to_dict()
        return {k: self._core_field.serialize(v) for k, v in data.items()}  # type: ignore[union-attr]


class Float(Field):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "float"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> float:
        return float(data)


class Integer(Field):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "integer"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[int, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> int:
        return int(data)


class Object(Field):
    """
    :arg doc_class: base doc class that handles mapping.
       If no `doc_class` is provided, new instance of `InnerDoc` will be created,
       populated with `properties` and used. Can not be provided together with `properties`
    :arg enabled:
    :arg subobjects:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "object"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        doc_class: Union[Type["InnerDoc"], "DefaultType"] = DEFAULT,
        *args: Any,
        enabled: Union[bool, "DefaultType"] = DEFAULT,
        subobjects: Union[
            Literal["true", "false", "auto"], bool, "DefaultType"
        ] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if enabled is not DEFAULT:
            kwargs["enabled"] = enabled
        if subobjects is not DEFAULT:
            kwargs["subobjects"] = subobjects
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep

        if doc_class is not DEFAULT and (
            properties is not DEFAULT or dynamic is not DEFAULT
        ):
            raise ValidationException(
                "doc_class and properties/dynamic should not be provided together"
            )
        if doc_class is not DEFAULT:
            self._doc_class: Type["InnerDoc"] = doc_class
        else:
            # FIXME import
            from .document import InnerDoc

            # no InnerDoc subclass, creating one instead...
            self._doc_class = type("InnerDoc", (InnerDoc,), {})
            for name, field in (
                properties if properties is not DEFAULT else {}
            ).items():
                self._doc_class._doc_type.mapping.field(name, field)
            if "properties" in kwargs:
                del kwargs["properties"]
            if dynamic is not DEFAULT:
                self._doc_class._doc_type.mapping.meta("dynamic", dynamic)

        self._mapping: "MappingBase" = deepcopy(self._doc_class._doc_type.mapping)
        super().__init__(**kwargs)

    def __getitem__(self, name: str) -> Field:
        return self._mapping[name]

    def __contains__(self, name: str) -> bool:
        return name in self._mapping

    def _empty(self) -> "InnerDoc":
        return self._wrap({})

    def _wrap(self, data: Dict[str, Any]) -> "InnerDoc":
        return self._doc_class.from_es(data, data_only=True)

    def empty(self) -> Union["InnerDoc", AttrList[Any]]:
        if self._multi:
            return AttrList[Any]([], self._wrap)
        return self._empty()

    def to_dict(self) -> Dict[str, Any]:
        d = self._mapping.to_dict()
        d.update(super().to_dict())
        return d

    def _collect_fields(self) -> Iterator[Field]:
        return self._mapping.properties._collect_fields()

    def _deserialize(self, data: Any) -> "InnerDoc":
        # don't wrap already wrapped data
        if isinstance(data, self._doc_class):
            return data

        if isinstance(data, AttrDict):
            data = data._d_

        return self._wrap(data)

    def _serialize(
        self, data: Optional[Union[Dict[str, Any], "InnerDoc"]], skip_empty: bool
    ) -> Optional[Dict[str, Any]]:
        if data is None:
            return None

        # somebody assigned raw dict to the field, we should tolerate that
        if isinstance(data, collections.abc.Mapping):
            return data

        return data.to_dict(skip_empty=skip_empty)

    def clean(self, data: Any) -> Any:
        data = super().clean(data)
        if data is None:
            return None
        if isinstance(data, (list, AttrList)):
            for d in cast(Iterator["InnerDoc"], data):
                d.full_clean()
        else:
            data.full_clean()
        return data

    def update(self, other: Any, update_only: bool = False) -> None:
        if not isinstance(other, Object):
            # not an inner/nested object, no merge possible
            return

        self._mapping.update(other._mapping, update_only)


class AggregateMetricDouble(Field):
    """
    :arg default_metric: (required)
    :arg metrics: (required)
    :arg ignore_malformed:
    :arg time_series_metric:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "aggregate_metric_double"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        default_metric: Union[str, "DefaultType"] = DEFAULT,
        metrics: Union[Sequence[str], "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if default_metric is not DEFAULT:
            kwargs["default_metric"] = default_metric
        if metrics is not DEFAULT:
            kwargs["metrics"] = metrics
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Alias(Field):
    """
    :arg path:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "alias"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        path: Union[str, "InstrumentedField", "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if path is not DEFAULT:
            kwargs["path"] = str(path)
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Binary(Field):
    """
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "binary"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)

    def clean(self, data: str) -> str:
        # Binary fields are opaque, so there's not much cleaning
        # that can be done.
        return data

    def _deserialize(self, data: Any) -> bytes:
        return base64.b64decode(data)

    def _serialize(self, data: Any, skip_empty: bool) -> Optional[str]:
        if data is None:
            return None
        return base64.b64encode(data).decode()


class Boolean(Field):
    """
    :arg boost:
    :arg fielddata:
    :arg index:
    :arg null_value:
    :arg ignore_malformed:
    :arg script:
    :arg on_script_error:
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "boolean"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        fielddata: Union[
            "types.NumericFielddata", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if fielddata is not DEFAULT:
            kwargs["fielddata"] = fielddata
        if index is not DEFAULT:
            kwargs["index"] = index
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if script is not DEFAULT:
            kwargs["script"] = script
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> bool:
        if data == "false":
            return False
        return bool(data)

    def clean(self, data: Any) -> Optional[bool]:
        if data is not None:
            data = self.deserialize(data)
        if data is None and self._required:
            raise ValidationException("Value required for this field.")
        return data  # type: ignore[no-any-return]


class Byte(Integer):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "byte"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Completion(Field):
    """
    :arg analyzer:
    :arg contexts:
    :arg max_input_length:
    :arg preserve_position_increments:
    :arg preserve_separators:
    :arg search_analyzer:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "completion"
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        contexts: Union[
            Sequence["types.SuggestContext"], Sequence[Dict[str, Any]], "DefaultType"
        ] = DEFAULT,
        max_input_length: Union[int, "DefaultType"] = DEFAULT,
        preserve_position_increments: Union[bool, "DefaultType"] = DEFAULT,
        preserve_separators: Union[bool, "DefaultType"] = DEFAULT,
        search_analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if contexts is not DEFAULT:
            kwargs["contexts"] = contexts
        if max_input_length is not DEFAULT:
            kwargs["max_input_length"] = max_input_length
        if preserve_position_increments is not DEFAULT:
            kwargs["preserve_position_increments"] = preserve_position_increments
        if preserve_separators is not DEFAULT:
            kwargs["preserve_separators"] = preserve_separators
        if search_analyzer is not DEFAULT:
            kwargs["search_analyzer"] = search_analyzer
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class ConstantKeyword(Field):
    """
    :arg value:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "constant_keyword"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        value: Any = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if value is not DEFAULT:
            kwargs["value"] = value
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class CountedKeyword(Field):
    """
    :arg index:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "counted_keyword"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        index: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if index is not DEFAULT:
            kwargs["index"] = index
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Date(Field):
    """
    :arg default_timezone: timezone that will be automatically used for tz-naive values
       May be instance of `datetime.tzinfo` or string containing TZ offset
    :arg boost:
    :arg fielddata:
    :arg format:
    :arg ignore_malformed:
    :arg index:
    :arg script:
    :arg on_script_error:
    :arg null_value:
    :arg precision_step:
    :arg locale:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "date"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        default_timezone: Union[str, "tzinfo", "DefaultType"] = DEFAULT,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        fielddata: Union[
            "types.NumericFielddata", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        null_value: Any = DEFAULT,
        precision_step: Union[int, "DefaultType"] = DEFAULT,
        locale: Union[str, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if fielddata is not DEFAULT:
            kwargs["fielddata"] = fielddata
        if format is not DEFAULT:
            kwargs["format"] = format
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if script is not DEFAULT:
            kwargs["script"] = script
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if precision_step is not DEFAULT:
            kwargs["precision_step"] = precision_step
        if locale is not DEFAULT:
            kwargs["locale"] = locale
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep

        if default_timezone is DEFAULT:
            self._default_timezone = None
        elif isinstance(default_timezone, str):
            self._default_timezone = tz.gettz(default_timezone)
        else:
            self._default_timezone = default_timezone
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> Union[datetime, date]:
        if isinstance(data, str):
            try:
                data = parser.parse(data)
            except Exception as e:
                raise ValidationException(
                    f"Could not parse date from the value ({data!r})", e
                )
            # we treat the yyyy-MM-dd format as a special case
            if hasattr(self, "format") and self.format == "yyyy-MM-dd":
                data = data.date()

        if isinstance(data, datetime):
            if self._default_timezone and data.tzinfo is None:
                data = data.replace(tzinfo=self._default_timezone)
            return cast(datetime, data)
        if isinstance(data, date):
            return data
        if isinstance(data, int):
            # Divide by a float to preserve milliseconds on the datetime.
            return datetime.utcfromtimestamp(data / 1000.0)

        raise ValidationException(f"Could not parse date from the value ({data!r})")


class DateNanos(Field):
    """
    :arg boost:
    :arg format:
    :arg ignore_malformed:
    :arg index:
    :arg script:
    :arg on_script_error:
    :arg null_value:
    :arg precision_step:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "date_nanos"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        format: Union[str, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        null_value: Any = DEFAULT,
        precision_step: Union[int, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if format is not DEFAULT:
            kwargs["format"] = format
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if script is not DEFAULT:
            kwargs["script"] = script
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if precision_step is not DEFAULT:
            kwargs["precision_step"] = precision_step
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class DateRange(RangeField):
    """
    :arg format:
    :arg boost:
    :arg coerce:
    :arg index:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "date_range"
    _core_field = Date()
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        format: Union[str, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if format is not DEFAULT:
            kwargs["format"] = format
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if index is not DEFAULT:
            kwargs["index"] = index
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class DenseVector(Field):
    """
    :arg dims: Number of vector dimensions. Can't exceed `4096`. If `dims`
        is not specified, it will be set to the length of the first vector
        added to the field.
    :arg element_type: The data type used to encode vectors. The supported
        data types are `float` (default), `byte`, and `bit`. Defaults to
        `float` if omitted.
    :arg index: If `true`, you can search this field using the kNN search
        API. Defaults to `True` if omitted.
    :arg index_options: An optional section that configures the kNN
        indexing algorithm. The HNSW algorithm has two internal parameters
        that influence how the data structure is built. These can be
        adjusted to improve the accuracy of results, at the expense of
        slower indexing speed.  This parameter can only be specified when
        `index` is `true`.
    :arg similarity: The vector similarity metric to use in kNN search.
        Documents are ranked by their vector field's similarity to the
        query vector. The `_score` of each document will be derived from
        the similarity, in a way that ensures scores are positive and that
        a larger score corresponds to a higher ranking.  Defaults to
        `l2_norm` when `element_type` is `bit` otherwise defaults to
        `cosine`.  `bit` vectors only support `l2_norm` as their
        similarity metric.  This parameter can only be specified when
        `index` is `true`.
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "dense_vector"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        dims: Union[int, "DefaultType"] = DEFAULT,
        element_type: Union[Literal["bit", "byte", "float"], "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            "types.DenseVectorIndexOptions", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        similarity: Union[
            Literal["cosine", "dot_product", "l2_norm", "max_inner_product"],
            "DefaultType",
        ] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if dims is not DEFAULT:
            kwargs["dims"] = dims
        if element_type is not DEFAULT:
            kwargs["element_type"] = element_type
        if index is not DEFAULT:
            kwargs["index"] = index
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if similarity is not DEFAULT:
            kwargs["similarity"] = similarity
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        self._element_type = kwargs.get("element_type", "float")
        if self._element_type in ["float", "byte"]:
            kwargs["multi"] = True
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> Any:
        if self._element_type == "float":
            return float(data)
        elif self._element_type == "byte":
            return int(data)
        return data


class Double(Float):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "double"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class DoubleRange(RangeField):
    """
    :arg boost:
    :arg coerce:
    :arg index:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "double_range"
    _core_field = Double()
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if index is not DEFAULT:
            kwargs["index"] = index
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Flattened(Field):
    """
    :arg boost:
    :arg depth_limit:
    :arg doc_values:
    :arg eager_global_ordinals:
    :arg index:
    :arg index_options:
    :arg null_value:
    :arg similarity:
    :arg split_queries_on_whitespace:
    :arg time_series_dimensions:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "flattened"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        depth_limit: Union[int, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        eager_global_ordinals: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            Literal["docs", "freqs", "positions", "offsets"], "DefaultType"
        ] = DEFAULT,
        null_value: Union[str, "DefaultType"] = DEFAULT,
        similarity: Union[str, "DefaultType"] = DEFAULT,
        split_queries_on_whitespace: Union[bool, "DefaultType"] = DEFAULT,
        time_series_dimensions: Union[Sequence[str], "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if depth_limit is not DEFAULT:
            kwargs["depth_limit"] = depth_limit
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if eager_global_ordinals is not DEFAULT:
            kwargs["eager_global_ordinals"] = eager_global_ordinals
        if index is not DEFAULT:
            kwargs["index"] = index
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if similarity is not DEFAULT:
            kwargs["similarity"] = similarity
        if split_queries_on_whitespace is not DEFAULT:
            kwargs["split_queries_on_whitespace"] = split_queries_on_whitespace
        if time_series_dimensions is not DEFAULT:
            kwargs["time_series_dimensions"] = time_series_dimensions
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class FloatRange(RangeField):
    """
    :arg boost:
    :arg coerce:
    :arg index:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "float_range"
    _core_field = Float()
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if index is not DEFAULT:
            kwargs["index"] = index
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class GeoPoint(Field):
    """
    :arg ignore_malformed:
    :arg ignore_z_value:
    :arg null_value:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "geo_point"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        ignore_z_value: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[
            "types.LatLonGeoLocation",
            "types.GeoHashLocation",
            Sequence[float],
            str,
            Dict[str, Any],
            "DefaultType",
        ] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "position"], "DefaultType"
        ] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if ignore_z_value is not DEFAULT:
            kwargs["ignore_z_value"] = ignore_z_value
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class GeoShape(Field):
    """
    The `geo_shape` data type facilitates the indexing of and searching
    with arbitrary geo shapes such as rectangles and polygons.

    :arg coerce:
    :arg ignore_malformed:
    :arg ignore_z_value:
    :arg index:
    :arg orientation:
    :arg strategy:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "geo_shape"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        ignore_z_value: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        orientation: Union[Literal["right", "left"], "DefaultType"] = DEFAULT,
        strategy: Union[Literal["recursive", "term"], "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if ignore_z_value is not DEFAULT:
            kwargs["ignore_z_value"] = ignore_z_value
        if index is not DEFAULT:
            kwargs["index"] = index
        if orientation is not DEFAULT:
            kwargs["orientation"] = orientation
        if strategy is not DEFAULT:
            kwargs["strategy"] = strategy
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class HalfFloat(Float):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "half_float"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Histogram(Field):
    """
    :arg ignore_malformed:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "histogram"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class IcuCollationKeyword(Field):
    """
    :arg norms:
    :arg index_options:
    :arg index: Should the field be searchable?
    :arg null_value: Accepts a string value which is substituted for any
        explicit null values. Defaults to null, which means the field is
        treated as missing.
    :arg rules:
    :arg language:
    :arg country:
    :arg variant:
    :arg strength:
    :arg decomposition:
    :arg alternate:
    :arg case_level:
    :arg case_first:
    :arg numeric:
    :arg variable_top:
    :arg hiragana_quaternary_mode:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "icu_collation_keyword"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        norms: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            Literal["docs", "freqs", "positions", "offsets"], "DefaultType"
        ] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[str, "DefaultType"] = DEFAULT,
        rules: Union[str, "DefaultType"] = DEFAULT,
        language: Union[str, "DefaultType"] = DEFAULT,
        country: Union[str, "DefaultType"] = DEFAULT,
        variant: Union[str, "DefaultType"] = DEFAULT,
        strength: Union[
            Literal["primary", "secondary", "tertiary", "quaternary", "identical"],
            "DefaultType",
        ] = DEFAULT,
        decomposition: Union[Literal["no", "identical"], "DefaultType"] = DEFAULT,
        alternate: Union[Literal["shifted", "non-ignorable"], "DefaultType"] = DEFAULT,
        case_level: Union[bool, "DefaultType"] = DEFAULT,
        case_first: Union[Literal["lower", "upper"], "DefaultType"] = DEFAULT,
        numeric: Union[bool, "DefaultType"] = DEFAULT,
        variable_top: Union[str, "DefaultType"] = DEFAULT,
        hiragana_quaternary_mode: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if norms is not DEFAULT:
            kwargs["norms"] = norms
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if index is not DEFAULT:
            kwargs["index"] = index
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if rules is not DEFAULT:
            kwargs["rules"] = rules
        if language is not DEFAULT:
            kwargs["language"] = language
        if country is not DEFAULT:
            kwargs["country"] = country
        if variant is not DEFAULT:
            kwargs["variant"] = variant
        if strength is not DEFAULT:
            kwargs["strength"] = strength
        if decomposition is not DEFAULT:
            kwargs["decomposition"] = decomposition
        if alternate is not DEFAULT:
            kwargs["alternate"] = alternate
        if case_level is not DEFAULT:
            kwargs["case_level"] = case_level
        if case_first is not DEFAULT:
            kwargs["case_first"] = case_first
        if numeric is not DEFAULT:
            kwargs["numeric"] = numeric
        if variable_top is not DEFAULT:
            kwargs["variable_top"] = variable_top
        if hiragana_quaternary_mode is not DEFAULT:
            kwargs["hiragana_quaternary_mode"] = hiragana_quaternary_mode
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class IntegerRange(RangeField):
    """
    :arg boost:
    :arg coerce:
    :arg index:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "integer_range"
    _core_field = Integer()
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if index is not DEFAULT:
            kwargs["index"] = index
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Ip(Field):
    """
    :arg boost:
    :arg index:
    :arg ignore_malformed:
    :arg null_value:
    :arg on_script_error:
    :arg script:
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "ip"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[str, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if index is not DEFAULT:
            kwargs["index"] = index
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> Union["IPv4Address", "IPv6Address"]:
        # the ipaddress library for pypy only accepts unicode.
        return ipaddress.ip_address(unicode(data))

    def _serialize(self, data: Any, skip_empty: bool) -> Optional[str]:
        if data is None:
            return None
        return str(data)


class IpRange(Field):
    """
    :arg boost:
    :arg coerce:
    :arg index:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "ip_range"
    _core_field = Ip()
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if index is not DEFAULT:
            kwargs["index"] = index
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Join(Field):
    """
    :arg relations:
    :arg eager_global_ordinals:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "join"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        relations: Union[
            Mapping[str, Union[str, Sequence[str]]], "DefaultType"
        ] = DEFAULT,
        eager_global_ordinals: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if relations is not DEFAULT:
            kwargs["relations"] = relations
        if eager_global_ordinals is not DEFAULT:
            kwargs["eager_global_ordinals"] = eager_global_ordinals
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Keyword(Field):
    """
    :arg boost:
    :arg eager_global_ordinals:
    :arg index:
    :arg index_options:
    :arg script:
    :arg on_script_error:
    :arg normalizer:
    :arg norms:
    :arg null_value:
    :arg similarity:
    :arg split_queries_on_whitespace:
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "keyword"
    _param_defs = {
        "normalizer": {"type": "normalizer"},
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        eager_global_ordinals: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            Literal["docs", "freqs", "positions", "offsets"], "DefaultType"
        ] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        normalizer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        norms: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[str, "DefaultType"] = DEFAULT,
        similarity: Union[str, None, "DefaultType"] = DEFAULT,
        split_queries_on_whitespace: Union[bool, "DefaultType"] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if eager_global_ordinals is not DEFAULT:
            kwargs["eager_global_ordinals"] = eager_global_ordinals
        if index is not DEFAULT:
            kwargs["index"] = index
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if script is not DEFAULT:
            kwargs["script"] = script
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if normalizer is not DEFAULT:
            kwargs["normalizer"] = normalizer
        if norms is not DEFAULT:
            kwargs["norms"] = norms
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if similarity is not DEFAULT:
            kwargs["similarity"] = similarity
        if split_queries_on_whitespace is not DEFAULT:
            kwargs["split_queries_on_whitespace"] = split_queries_on_whitespace
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Long(Integer):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "long"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[int, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class LongRange(RangeField):
    """
    :arg boost:
    :arg coerce:
    :arg index:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "long_range"
    _core_field = Long()
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if index is not DEFAULT:
            kwargs["index"] = index
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class MatchOnlyText(Field):
    """
    A variant of text that trades scoring and efficiency of positional
    queries for space efficiency. This field effectively stores data the
    same way as a text field that only indexes documents (index_options:
    docs) and disables norms (norms: false). Term queries perform as fast
    if not faster as on text fields, however queries that need positions
    such as the match_phrase query perform slower as they need to look at
    the _source document to verify whether a phrase matches. All queries
    return constant scores that are equal to 1.0.

    :arg fields:
    :arg meta: Metadata about the field.
    :arg copy_to: Allows you to copy the values of multiple fields into a
        group field, which can then be queried as a single field.
    """

    name = "match_only_text"
    _param_defs = {
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        super().__init__(*args, **kwargs)


class Murmur3(Field):
    """
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "murmur3"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Nested(Object):
    """
    :arg enabled:
    :arg include_in_parent:
    :arg include_in_root:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "nested"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        enabled: Union[bool, "DefaultType"] = DEFAULT,
        include_in_parent: Union[bool, "DefaultType"] = DEFAULT,
        include_in_root: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if enabled is not DEFAULT:
            kwargs["enabled"] = enabled
        if include_in_parent is not DEFAULT:
            kwargs["include_in_parent"] = include_in_parent
        if include_in_root is not DEFAULT:
            kwargs["include_in_root"] = include_in_root
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        kwargs.setdefault("multi", True)
        super().__init__(*args, **kwargs)


class Passthrough(Field):
    """
    :arg enabled:
    :arg priority:
    :arg time_series_dimension:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "passthrough"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        enabled: Union[bool, "DefaultType"] = DEFAULT,
        priority: Union[int, "DefaultType"] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if enabled is not DEFAULT:
            kwargs["enabled"] = enabled
        if priority is not DEFAULT:
            kwargs["priority"] = priority
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Percolator(Field):
    """
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "percolator"
    _coerce = True
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> "Query":
        return Q(data)  # type: ignore[no-any-return]

    def _serialize(self, data: Any, skip_empty: bool) -> Optional[Dict[str, Any]]:
        if data is None:
            return None
        return data.to_dict()  # type: ignore[no-any-return]


class Point(Field):
    """
    :arg ignore_malformed:
    :arg ignore_z_value:
    :arg null_value:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "point"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        ignore_z_value: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[str, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if ignore_z_value is not DEFAULT:
            kwargs["ignore_z_value"] = ignore_z_value
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class RankFeature(Float):
    """
    :arg positive_score_impact:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "rank_feature"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        positive_score_impact: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if positive_score_impact is not DEFAULT:
            kwargs["positive_score_impact"] = positive_score_impact
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class RankFeatures(Field):
    """
    :arg positive_score_impact:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "rank_features"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        positive_score_impact: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if positive_score_impact is not DEFAULT:
            kwargs["positive_score_impact"] = positive_score_impact
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class RankVectors(Field):
    """
    Technical preview

    :arg element_type:
    :arg dims:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "rank_vectors"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        element_type: Union[Literal["byte", "float", "bit"], "DefaultType"] = DEFAULT,
        dims: Union[int, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if element_type is not DEFAULT:
            kwargs["element_type"] = element_type
        if dims is not DEFAULT:
            kwargs["dims"] = dims
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class ScaledFloat(Float):
    """
    :arg null_value:
    :arg scaling_factor:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "scaled_float"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        scaling_factor: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if scaling_factor is not DEFAULT:
            kwargs["scaling_factor"] = scaling_factor
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        if "scaling_factor" not in kwargs:
            if len(args) > 0:
                kwargs["scaling_factor"] = args[0]
                args = args[1:]
            else:
                raise TypeError("missing required argument: 'scaling_factor'")
        super().__init__(*args, **kwargs)


class SearchAsYouType(Field):
    """
    :arg analyzer:
    :arg index:
    :arg index_options:
    :arg max_shingle_size:
    :arg norms:
    :arg search_analyzer:
    :arg search_quote_analyzer:
    :arg similarity:
    :arg term_vector:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "search_as_you_type"
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "search_quote_analyzer": {"type": "analyzer"},
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            Literal["docs", "freqs", "positions", "offsets"], "DefaultType"
        ] = DEFAULT,
        max_shingle_size: Union[int, "DefaultType"] = DEFAULT,
        norms: Union[bool, "DefaultType"] = DEFAULT,
        search_analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        search_quote_analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        similarity: Union[str, None, "DefaultType"] = DEFAULT,
        term_vector: Union[
            Literal[
                "no",
                "yes",
                "with_offsets",
                "with_positions",
                "with_positions_offsets",
                "with_positions_offsets_payloads",
                "with_positions_payloads",
            ],
            "DefaultType",
        ] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if index is not DEFAULT:
            kwargs["index"] = index
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if max_shingle_size is not DEFAULT:
            kwargs["max_shingle_size"] = max_shingle_size
        if norms is not DEFAULT:
            kwargs["norms"] = norms
        if search_analyzer is not DEFAULT:
            kwargs["search_analyzer"] = search_analyzer
        if search_quote_analyzer is not DEFAULT:
            kwargs["search_quote_analyzer"] = search_quote_analyzer
        if similarity is not DEFAULT:
            kwargs["similarity"] = similarity
        if term_vector is not DEFAULT:
            kwargs["term_vector"] = term_vector
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class SemanticText(Field):
    """
    :arg meta:
    :arg inference_id: Inference endpoint that will be used to generate
        embeddings for the field. This parameter cannot be updated. Use
        the Create inference API to create the endpoint. If
        `search_inference_id` is specified, the inference endpoint will
        only be used at index time. Defaults to `.elser-2-elasticsearch`
        if omitted.
    :arg search_inference_id: Inference endpoint that will be used to
        generate embeddings at query time. You can update this parameter
        by using the Update mapping API. Use the Create inference API to
        create the endpoint. If not specified, the inference endpoint
        defined by inference_id will be used at both index and query time.
    :arg chunking_settings: Settings for chunking text into smaller
        passages. If specified, these will override the chunking settings
        sent in the inference endpoint associated with inference_id. If
        chunking settings are updated, they will not be applied to
        existing documents until they are reindexed.
    """

    name = "semantic_text"

    def __init__(
        self,
        *args: Any,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        inference_id: Union[str, "DefaultType"] = DEFAULT,
        search_inference_id: Union[str, "DefaultType"] = DEFAULT,
        chunking_settings: Union[
            "types.ChunkingSettings", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if inference_id is not DEFAULT:
            kwargs["inference_id"] = inference_id
        if search_inference_id is not DEFAULT:
            kwargs["search_inference_id"] = search_inference_id
        if chunking_settings is not DEFAULT:
            kwargs["chunking_settings"] = chunking_settings
        super().__init__(*args, **kwargs)


class Shape(Field):
    """
    The `shape` data type facilitates the indexing of and searching with
    arbitrary `x, y` cartesian shapes such as rectangles and polygons.

    :arg coerce:
    :arg ignore_malformed:
    :arg ignore_z_value:
    :arg orientation:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "shape"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        ignore_z_value: Union[bool, "DefaultType"] = DEFAULT,
        orientation: Union[Literal["right", "left"], "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if ignore_z_value is not DEFAULT:
            kwargs["ignore_z_value"] = ignore_z_value
        if orientation is not DEFAULT:
            kwargs["orientation"] = orientation
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Short(Integer):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "short"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class SparseVector(Field):
    """
    :arg store:
    :arg index_options: Additional index options for the sparse vector
        field that controls the token pruning behavior of the sparse
        vector field.
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "sparse_vector"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        store: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            "types.SparseVectorIndexOptions", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if store is not DEFAULT:
            kwargs["store"] = store
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Text(Field):
    """
    :arg analyzer:
    :arg boost:
    :arg eager_global_ordinals:
    :arg fielddata:
    :arg fielddata_frequency_filter:
    :arg index:
    :arg index_options:
    :arg index_phrases:
    :arg index_prefixes:
    :arg norms:
    :arg position_increment_gap:
    :arg search_analyzer:
    :arg search_quote_analyzer:
    :arg similarity:
    :arg term_vector:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "text"
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "search_quote_analyzer": {"type": "analyzer"},
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        eager_global_ordinals: Union[bool, "DefaultType"] = DEFAULT,
        fielddata: Union[bool, "DefaultType"] = DEFAULT,
        fielddata_frequency_filter: Union[
            "types.FielddataFrequencyFilter", Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        index_options: Union[
            Literal["docs", "freqs", "positions", "offsets"], "DefaultType"
        ] = DEFAULT,
        index_phrases: Union[bool, "DefaultType"] = DEFAULT,
        index_prefixes: Union[
            "types.TextIndexPrefixes", None, Dict[str, Any], "DefaultType"
        ] = DEFAULT,
        norms: Union[bool, "DefaultType"] = DEFAULT,
        position_increment_gap: Union[int, "DefaultType"] = DEFAULT,
        search_analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        search_quote_analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        similarity: Union[str, None, "DefaultType"] = DEFAULT,
        term_vector: Union[
            Literal[
                "no",
                "yes",
                "with_offsets",
                "with_positions",
                "with_positions_offsets",
                "with_positions_offsets_payloads",
                "with_positions_payloads",
            ],
            "DefaultType",
        ] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if eager_global_ordinals is not DEFAULT:
            kwargs["eager_global_ordinals"] = eager_global_ordinals
        if fielddata is not DEFAULT:
            kwargs["fielddata"] = fielddata
        if fielddata_frequency_filter is not DEFAULT:
            kwargs["fielddata_frequency_filter"] = fielddata_frequency_filter
        if index is not DEFAULT:
            kwargs["index"] = index
        if index_options is not DEFAULT:
            kwargs["index_options"] = index_options
        if index_phrases is not DEFAULT:
            kwargs["index_phrases"] = index_phrases
        if index_prefixes is not DEFAULT:
            kwargs["index_prefixes"] = index_prefixes
        if norms is not DEFAULT:
            kwargs["norms"] = norms
        if position_increment_gap is not DEFAULT:
            kwargs["position_increment_gap"] = position_increment_gap
        if search_analyzer is not DEFAULT:
            kwargs["search_analyzer"] = search_analyzer
        if search_quote_analyzer is not DEFAULT:
            kwargs["search_quote_analyzer"] = search_quote_analyzer
        if similarity is not DEFAULT:
            kwargs["similarity"] = similarity
        if term_vector is not DEFAULT:
            kwargs["term_vector"] = term_vector
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class TokenCount(Field):
    """
    :arg analyzer:
    :arg boost:
    :arg index:
    :arg null_value:
    :arg enable_position_increments:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "token_count"
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        analyzer: Union[str, DslBase, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        null_value: Union[float, "DefaultType"] = DEFAULT,
        enable_position_increments: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if analyzer is not DEFAULT:
            kwargs["analyzer"] = analyzer
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if index is not DEFAULT:
            kwargs["index"] = index
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if enable_position_increments is not DEFAULT:
            kwargs["enable_position_increments"] = enable_position_increments
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class UnsignedLong(Field):
    """
    :arg null_value:
    :arg boost:
    :arg coerce:
    :arg ignore_malformed:
    :arg index:
    :arg on_script_error:
    :arg script:
    :arg time_series_metric: For internal use by Elastic only. Marks the
        field as a time series dimension. Defaults to false.
    :arg time_series_dimension: For internal use by Elastic only. Marks
        the field as a time series dimension. Defaults to false.
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "unsigned_long"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[int, "DefaultType"] = DEFAULT,
        boost: Union[float, "DefaultType"] = DEFAULT,
        coerce: Union[bool, "DefaultType"] = DEFAULT,
        ignore_malformed: Union[bool, "DefaultType"] = DEFAULT,
        index: Union[bool, "DefaultType"] = DEFAULT,
        on_script_error: Union[Literal["fail", "continue"], "DefaultType"] = DEFAULT,
        script: Union["types.Script", Dict[str, Any], "DefaultType"] = DEFAULT,
        time_series_metric: Union[
            Literal["gauge", "counter", "summary", "histogram", "position"],
            "DefaultType",
        ] = DEFAULT,
        time_series_dimension: Union[bool, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if boost is not DEFAULT:
            kwargs["boost"] = boost
        if coerce is not DEFAULT:
            kwargs["coerce"] = coerce
        if ignore_malformed is not DEFAULT:
            kwargs["ignore_malformed"] = ignore_malformed
        if index is not DEFAULT:
            kwargs["index"] = index
        if on_script_error is not DEFAULT:
            kwargs["on_script_error"] = on_script_error
        if script is not DEFAULT:
            kwargs["script"] = script
        if time_series_metric is not DEFAULT:
            kwargs["time_series_metric"] = time_series_metric
        if time_series_dimension is not DEFAULT:
            kwargs["time_series_dimension"] = time_series_dimension
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Version(Field):
    """
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "version"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)


class Wildcard(Field):
    """
    :arg null_value:
    :arg doc_values:
    :arg copy_to:
    :arg store:
    :arg meta: Metadata about the field.
    :arg properties:
    :arg ignore_above:
    :arg dynamic:
    :arg fields:
    :arg synthetic_source_keep:
    """

    name = "wildcard"
    _param_defs = {
        "properties": {"type": "field", "hash": True},
        "fields": {"type": "field", "hash": True},
    }

    def __init__(
        self,
        *args: Any,
        null_value: Union[str, "DefaultType"] = DEFAULT,
        doc_values: Union[bool, "DefaultType"] = DEFAULT,
        copy_to: Union[
            Union[str, "InstrumentedField"],
            Sequence[Union[str, "InstrumentedField"]],
            "DefaultType",
        ] = DEFAULT,
        store: Union[bool, "DefaultType"] = DEFAULT,
        meta: Union[Mapping[str, str], "DefaultType"] = DEFAULT,
        properties: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        ignore_above: Union[int, "DefaultType"] = DEFAULT,
        dynamic: Union[
            Literal["strict", "runtime", "true", "false"], bool, "DefaultType"
        ] = DEFAULT,
        fields: Union[Mapping[str, Field], "DefaultType"] = DEFAULT,
        synthetic_source_keep: Union[
            Literal["none", "arrays", "all"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if null_value is not DEFAULT:
            kwargs["null_value"] = null_value
        if doc_values is not DEFAULT:
            kwargs["doc_values"] = doc_values
        if copy_to is not DEFAULT:
            if isinstance(copy_to, list):
                kwargs["copy_to"] = [str(field) for field in copy_to]
            else:
                kwargs["copy_to"] = str(copy_to)
        if store is not DEFAULT:
            kwargs["store"] = store
        if meta is not DEFAULT:
            kwargs["meta"] = meta
        if properties is not DEFAULT:
            kwargs["properties"] = properties
        if ignore_above is not DEFAULT:
            kwargs["ignore_above"] = ignore_above
        if dynamic is not DEFAULT:
            kwargs["dynamic"] = dynamic
        if fields is not DEFAULT:
            kwargs["fields"] = fields
        if synthetic_source_keep is not DEFAULT:
            kwargs["synthetic_source_keep"] = synthetic_source_keep
        super().__init__(*args, **kwargs)
