# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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
import collections.abc as collections_abc
import copy
import ipaddress
from datetime import date, datetime
from typing import Any, Optional, Type

from dateutil import parser, tz

from ..exceptions import ValidationException
from .query import Q
from .utils import AttrDict, AttrList, DslBase
from .wrappers import Range

# pylint: disable=invalid-name
unicode: Type[str] = str


def construct_field(name_or_field: Any, **params: Any) -> Any:
    # {"type": "text", "analyzer": "snowball"}
    if isinstance(name_or_field, collections_abc.Mapping):
        if params:
            raise ValueError(
                "construct_field() cannot accept parameters when passing in a dict."
            )
        params = name_or_field.copy()  # type: ignore
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
    _type_name: str = "field"
    _type_shortcut = staticmethod(construct_field)
    # all fields can be multifields
    _param_defs = {"fields": {"type": "field", "hash": True}}
    name: Optional[str] = None
    _coerce: bool = False

    def __init__(
        self, multi: bool = False, required: bool = False, *args: Any, **kwargs: Any
    ) -> None:
        """
        :arg bool multi: specifies whether field can contain array of values
        :arg bool required: specifies whether field is required
        """
        self._multi = multi
        self._required = required
        super().__init__(*args, **kwargs)

    def __getitem__(self, subfield: Any) -> Any:
        return self._params.get("fields", {})[subfield]

    def _serialize(self, data: Any) -> Any:
        return data

    def _deserialize(self, data: Any) -> Any:
        return data

    def _empty(self) -> None:
        return None

    def empty(self) -> Any:
        if self._multi:
            return AttrList([])
        return self._empty()

    def serialize(self, data: Any) -> Any:
        if isinstance(data, (list, AttrList, tuple)):
            return list(map(self._serialize, data))
        return self._serialize(data)

    def deserialize(self, data: Any) -> Any:
        if isinstance(data, (list, AttrList, tuple)):
            data = [None if d is None else self._deserialize(d) for d in data]
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

    def to_dict(self) -> Any:
        d = super().to_dict()
        name, value = d.popitem()
        value["type"] = name
        return value


class CustomField(Field):
    name = "custom"
    _coerce = True

    def to_dict(self) -> Any:
        if isinstance(self.builtin_type, Field):
            return self.builtin_type.to_dict()

        d = super().to_dict()
        d["type"] = self.builtin_type
        return d


class Object(Field):
    name: Optional[str] = "object"
    _coerce: bool = True

    def __init__(
        self,
        doc_class: Any = None,
        dynamic: Any = None,
        properties: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        :arg document.InnerDoc doc_class: base doc class that handles mapping.
            If no `doc_class` is provided, new instance of `InnerDoc` will be created,
            populated with `properties` and used. Can not be provided together with `properties`
        :arg dynamic: whether new properties may be created dynamically.
            Valid values are `True`, `False`, `'strict'`.
            Can not be provided together with `doc_class`.
        :arg dict properties: used to construct underlying mapping if no `doc_class` is provided.
            Can not be provided together with `doc_class`
        """
        if doc_class and (properties or dynamic is not None):
            raise ValidationException(
                "doc_class and properties/dynamic should not be provided together"
            )
        if doc_class:
            self._doc_class: Any = doc_class
        else:
            # FIXME import
            from opensearchpy.helpers.document import InnerDoc

            # no InnerDoc subclass, creating one instead...
            self._doc_class = type("InnerDoc", (InnerDoc,), {})
            for name, field in (properties or {}).items():
                self._doc_class._doc_type.mapping.field(name, field)
            if dynamic is not None:
                self._doc_class._doc_type.mapping.meta("dynamic", dynamic)

        self._mapping = copy.deepcopy(self._doc_class._doc_type.mapping)
        super().__init__(**kwargs)

    def __getitem__(self, name: Any) -> Any:
        return self._mapping[name]

    def __contains__(self, name: Any) -> bool:
        return name in self._mapping

    def _empty(self) -> Any:
        return self._wrap({})

    def _wrap(self, data: Any) -> Any:
        return self._doc_class.from_opensearch(data, data_only=True)

    def empty(self) -> Any:
        if self._multi:
            return AttrList([], self._wrap)
        return self._empty()

    def to_dict(self) -> Any:
        d = self._mapping.to_dict()
        d.update(super().to_dict())
        return d

    def _collect_fields(self) -> Any:
        return self._mapping.properties._collect_fields()

    def _deserialize(self, data: Any) -> Any:
        # don't wrap already wrapped data
        if isinstance(data, self._doc_class):
            return data

        if isinstance(data, AttrDict):
            data = data._d_

        return self._wrap(data)

    def _serialize(self, data: Any) -> Any:
        if data is None:
            return None

        # somebody assigned raw dict to the field, we should tolerate that
        if isinstance(data, collections_abc.Mapping):
            return data

        return data.to_dict()

    def clean(self, data: Any) -> Any:
        data = super().clean(data)
        if data is None:
            return None
        if isinstance(data, (list, AttrList)):
            for d in data:
                d.full_clean()
        else:
            data.full_clean()
        return data

    def update(self, other: "Object", update_only: bool = False) -> None:
        if not isinstance(other, Object):
            # not an inner/nested object, no merge possible
            return

        self._mapping.update(other._mapping, update_only)


class Nested(Object):
    name: Optional[str] = "nested"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("multi", True)
        super().__init__(*args, **kwargs)


class Date(Field):
    name: Optional[str] = "date"
    _coerce: bool = True

    def __init__(self, default_timezone: Any = None, *args: Any, **kwargs: Any) -> None:
        """
        :arg default_timezone: timezone that will be automatically used for tz-naive values
            May be instance of `datetime.tzinfo` or string containing TZ offset
        """
        self._default_timezone = default_timezone
        if isinstance(self._default_timezone, str):
            self._default_timezone = tz.gettz(self._default_timezone)
        super().__init__(*args, **kwargs)

    def _deserialize(self, data: Any) -> Any:
        if isinstance(data, str):
            try:
                data = parser.parse(data)
            except Exception as e:
                raise ValidationException(
                    f"Could not parse date from the value ({data!r})", e
                )

        if isinstance(data, datetime):
            if self._default_timezone and data.tzinfo is None:
                data = data.replace(tzinfo=self._default_timezone)
            return data
        if isinstance(data, date):
            return data
        if isinstance(data, int):
            # Divide by a float to preserve milliseconds on the datetime.
            return datetime.utcfromtimestamp(data / 1000.0)

        raise ValidationException(f"Could not parse date from the value ({data!r})")


class Text(Field):
    _param_defs = {
        "fields": {"type": "field", "hash": True},
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "search_quote_analyzer": {"type": "analyzer"},
    }
    name: Optional[str] = "text"


class SearchAsYouType(Field):
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "search_quote_analyzer": {"type": "analyzer"},
    }
    name: Optional[str] = "search_as_you_type"


class Keyword(Field):
    _param_defs = {
        "fields": {"type": "field", "hash": True},
        "search_analyzer": {"type": "analyzer"},
        "normalizer": {"type": "normalizer"},
    }
    name: Optional[str] = "keyword"


class ConstantKeyword(Keyword):
    name: Optional[str] = "constant_keyword"


class Boolean(Field):
    name: Optional[str] = "boolean"
    _coerce: bool = True

    def _deserialize(self, data: Any) -> Any:
        if data == "false":
            return False
        return bool(data)

    def clean(self, data: Any) -> Any:
        if data is not None:
            data = self.deserialize(data)
        if data is None and self._required:
            raise ValidationException("Value required for this field.")
        return data


class Float(Field):
    name: Optional[str] = "float"
    _coerce: bool = True

    def _deserialize(self, data: Any) -> Any:
        return float(data)


class KnnVector(Float):
    name: Optional[str] = "knn_vector"

    def __init__(self, dimension: Any, **kwargs: Any) -> None:
        kwargs["multi"] = True
        super().__init__(dimension=dimension, **kwargs)


class SparseVector(Field):
    name: Optional[str] = "sparse_vector"


class HalfFloat(Float):
    name: Optional[str] = "half_float"


class ScaledFloat(Float):
    name: Optional[str] = "scaled_float"

    def __init__(self, scaling_factor: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(scaling_factor=scaling_factor, *args, **kwargs)


class Double(Float):
    name: Optional[str] = "double"


class RankFeature(Float):
    name: Optional[str] = "rank_feature"


class RankFeatures(Field):
    name: Optional[str] = "rank_features"


class Integer(Field):
    name: Optional[str] = "integer"
    _coerce: bool = True

    def _deserialize(self, data: Any) -> Any:
        return int(data)


class Byte(Integer):
    name: Optional[str] = "byte"


class Short(Integer):
    name: Optional[str] = "short"


class Long(Integer):
    name: Optional[str] = "long"


class Ip(Field):
    name: Optional[str] = "ip"
    _coerce: bool = True

    def _deserialize(self, data: Any) -> Any:
        # the ipaddress library for pypy only accepts unicode.
        return ipaddress.ip_address(unicode(data))

    def _serialize(self, data: Any) -> Any:
        if data is None:
            return None
        return str(data)


class Binary(Field):
    name: Optional[str] = "binary"
    _coerce: bool = True

    def clean(self, data: Any) -> Any:
        # Binary fields are opaque, so there's not much cleaning
        # that can be done.
        return data

    def _deserialize(self, data: Any) -> Any:
        return base64.b64decode(data)

    def _serialize(self, data: Any) -> Any:
        if data is None:
            return None
        return base64.b64encode(data).decode()


class GeoPoint(Field):
    name: Optional[str] = "geo_point"


class GeoShape(Field):
    name: Optional[str] = "geo_shape"


class Completion(Field):
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
    }
    name = "completion"


class Percolator(Field):
    name: Optional[str] = "percolator"
    _coerce: bool = True

    def _deserialize(self, data: Any) -> Any:
        return Q(data)

    def _serialize(self, data: Any) -> Any:
        if data is None:
            return None
        return data.to_dict()


class RangeField(Field):
    _coerce: bool = True
    _core_field: Any = None

    def _deserialize(self, data: Any) -> Any:
        if isinstance(data, Range):
            return data
        data = {k: self._core_field.deserialize(v) for k, v in data.items()}
        return Range(data)

    def _serialize(self, data: Any) -> Any:
        if data is None:
            return None
        if not isinstance(data, collections_abc.Mapping):
            data = data.to_dict()
        return {k: self._core_field.serialize(v) for k, v in data.items()}


class IntegerRange(RangeField):
    name: Optional[str] = "integer_range"
    _core_field: Any = Integer()


class FloatRange(RangeField):
    name: Optional[str] = "float_range"
    _core_field: Any = Float()


class LongRange(RangeField):
    name: Optional[str] = "long_range"
    _core_field: Any = Long()


class DoubleRange(RangeField):
    name: Optional[str] = "double_range"
    _core_field: Any = Double()


class DateRange(RangeField):
    name: Optional[str] = "date_range"
    _core_field: Any = Date()


class IpRange(Field):
    # not a RangeField since ip_range supports CIDR ranges
    name: Optional[str] = "ip_range"


class Join(Field):
    name: Optional[str] = "join"


class TokenCount(Field):
    name: Optional[str] = "token_count"


class Murmur3(Field):
    name: Optional[str] = "murmur3"
