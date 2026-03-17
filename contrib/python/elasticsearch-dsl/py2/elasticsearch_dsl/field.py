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
import copy
import ipaddress

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:
    import collections as collections_abc

from datetime import date, datetime

from dateutil import parser, tz
from six import integer_types, iteritems, string_types
from six.moves import map

from .exceptions import ValidationException
from .query import Q
from .utils import AttrDict, AttrList, DslBase
from .wrappers import Range

unicode = type(u"")


def construct_field(name_or_field, **params):
    # {"type": "text", "analyzer": "snowball"}
    if isinstance(name_or_field, collections_abc.Mapping):
        if params:
            raise ValueError(
                "construct_field() cannot accept parameters when passing in a dict."
            )
        params = name_or_field.copy()
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
    name = None
    _coerce = False

    def __init__(self, multi=False, required=False, *args, **kwargs):
        """
        :arg bool multi: specifies whether field can contain array of values
        :arg bool required: specifies whether field is required
        """
        self._multi = multi
        self._required = required
        super(Field, self).__init__(*args, **kwargs)

    def __getitem__(self, subfield):
        return self._params.get("fields", {})[subfield]

    def _serialize(self, data):
        return data

    def _deserialize(self, data):
        return data

    def _empty(self):
        return None

    def empty(self):
        if self._multi:
            return AttrList([])
        return self._empty()

    def serialize(self, data):
        if isinstance(data, (list, AttrList, tuple)):
            return list(map(self._serialize, data))
        return self._serialize(data)

    def deserialize(self, data):
        if isinstance(data, (list, AttrList, tuple)):
            data = [None if d is None else self._deserialize(d) for d in data]
            return data
        if data is None:
            return None
        return self._deserialize(data)

    def clean(self, data):
        if data is not None:
            data = self.deserialize(data)
        if data in (None, [], {}) and self._required:
            raise ValidationException("Value required for this field.")
        return data

    def to_dict(self):
        d = super(Field, self).to_dict()
        name, value = d.popitem()
        value["type"] = name
        return value


class CustomField(Field):
    name = "custom"
    _coerce = True

    def to_dict(self):
        if isinstance(self.builtin_type, Field):
            return self.builtin_type.to_dict()

        d = super(CustomField, self).to_dict()
        d["type"] = self.builtin_type
        return d


class Object(Field):
    name = "object"
    _coerce = True

    def __init__(self, doc_class=None, dynamic=None, properties=None, **kwargs):
        """
        :arg document.InnerDoc doc_class: base doc class that handles mapping.
            If no `doc_class` is provided, new instance of `InnerDoc` will be created,
            populated with `properties` and used. Can not be provided together with `properties`
        :arg dynamic: whether new properties may be created dynamically.
            Valid values are `True`, `False`, `'strict'`.
            Can not be provided together with `doc_class`.
            See https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html
            for more details
        :arg dict properties: used to construct underlying mapping if no `doc_class` is provided.
            Can not be provided together with `doc_class`
        """
        if doc_class and (properties or dynamic is not None):
            raise ValidationException(
                "doc_class and properties/dynamic should not be provided together"
            )
        if doc_class:
            self._doc_class = doc_class
        else:
            # FIXME import
            from .document import InnerDoc

            # no InnerDoc subclass, creating one instead...
            self._doc_class = type("InnerDoc", (InnerDoc,), {})
            for name, field in iteritems(properties or {}):
                self._doc_class._doc_type.mapping.field(name, field)
            if dynamic is not None:
                self._doc_class._doc_type.mapping.meta("dynamic", dynamic)

        self._mapping = copy.deepcopy(self._doc_class._doc_type.mapping)
        super(Object, self).__init__(**kwargs)

    def __getitem__(self, name):
        return self._mapping[name]

    def __contains__(self, name):
        return name in self._mapping

    def _empty(self):
        return self._wrap({})

    def _wrap(self, data):
        return self._doc_class.from_es(data, data_only=True)

    def empty(self):
        if self._multi:
            return AttrList([], self._wrap)
        return self._empty()

    def to_dict(self):
        d = self._mapping.to_dict()
        d.update(super(Object, self).to_dict())
        return d

    def _collect_fields(self):
        return self._mapping.properties._collect_fields()

    def _deserialize(self, data):
        # don't wrap already wrapped data
        if isinstance(data, self._doc_class):
            return data

        if isinstance(data, AttrDict):
            data = data._d_

        return self._wrap(data)

    def _serialize(self, data):
        if data is None:
            return None

        # somebody assigned raw dict to the field, we should tolerate that
        if isinstance(data, collections_abc.Mapping):
            return data

        return data.to_dict()

    def clean(self, data):
        data = super(Object, self).clean(data)
        if data is None:
            return None
        if isinstance(data, (list, AttrList)):
            for d in data:
                d.full_clean()
        else:
            data.full_clean()
        return data

    def update(self, other, update_only=False):
        if not isinstance(other, Object):
            # not an inner/nested object, no merge possible
            return

        self._mapping.update(other._mapping, update_only)


class Nested(Object):
    name = "nested"

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("multi", True)
        super(Nested, self).__init__(*args, **kwargs)


class Date(Field):
    name = "date"
    _coerce = True

    def __init__(self, default_timezone=None, *args, **kwargs):
        """
        :arg default_timezone: timezone that will be automatically used for tz-naive values
            May be instance of `datetime.tzinfo` or string containing TZ offset
        """
        self._default_timezone = default_timezone
        if isinstance(self._default_timezone, string_types):
            self._default_timezone = tz.gettz(self._default_timezone)
        super(Date, self).__init__(*args, **kwargs)

    def _deserialize(self, data):
        if isinstance(data, string_types):
            try:
                data = parser.parse(data)
            except Exception as e:
                raise ValidationException(
                    "Could not parse date from the value (%r)" % data, e
                )

        if isinstance(data, datetime):
            if self._default_timezone and data.tzinfo is None:
                data = data.replace(tzinfo=self._default_timezone)
            return data
        if isinstance(data, date):
            return data
        if isinstance(data, integer_types):
            # Divide by a float to preserve milliseconds on the datetime.
            return datetime.utcfromtimestamp(data / 1000.0)

        raise ValidationException("Could not parse date from the value (%r)" % data)


class Text(Field):
    _param_defs = {
        "fields": {"type": "field", "hash": True},
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "search_quote_analyzer": {"type": "analyzer"},
    }
    name = "text"


class SearchAsYouType(Field):
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
        "search_quote_analyzer": {"type": "analyzer"},
    }
    name = "search_as_you_type"


class Keyword(Field):
    _param_defs = {
        "fields": {"type": "field", "hash": True},
        "search_analyzer": {"type": "analyzer"},
        "normalizer": {"type": "normalizer"},
    }
    name = "keyword"


class ConstantKeyword(Keyword):
    name = "constant_keyword"


class Boolean(Field):
    name = "boolean"
    _coerce = True

    def _deserialize(self, data):
        if data == "false":
            return False
        return bool(data)

    def clean(self, data):
        if data is not None:
            data = self.deserialize(data)
        if data is None and self._required:
            raise ValidationException("Value required for this field.")
        return data


class Float(Field):
    name = "float"
    _coerce = True

    def _deserialize(self, data):
        return float(data)


class DenseVector(Float):
    name = "dense_vector"

    def __init__(self, dims, **kwargs):
        kwargs["multi"] = True
        super(DenseVector, self).__init__(dims=dims, **kwargs)


class SparseVector(Field):
    name = "sparse_vector"


class HalfFloat(Float):
    name = "half_float"


class ScaledFloat(Float):
    name = "scaled_float"

    def __init__(self, scaling_factor, *args, **kwargs):
        super(ScaledFloat, self).__init__(
            scaling_factor=scaling_factor, *args, **kwargs
        )


class Double(Float):
    name = "double"


class RankFeature(Float):
    name = "rank_feature"


class RankFeatures(Field):
    name = "rank_features"


class Integer(Field):
    name = "integer"
    _coerce = True

    def _deserialize(self, data):
        return int(data)


class Byte(Integer):
    name = "byte"


class Short(Integer):
    name = "short"


class Long(Integer):
    name = "long"


class Ip(Field):
    name = "ip"
    _coerce = True

    def _deserialize(self, data):
        # the ipaddress library for pypy only accepts unicode.
        return ipaddress.ip_address(unicode(data))

    def _serialize(self, data):
        if data is None:
            return None
        return str(data)


class Binary(Field):
    name = "binary"
    _coerce = True

    def clean(self, data):
        # Binary fields are opaque, so there's not much cleaning
        # that can be done.
        return data

    def _deserialize(self, data):
        return base64.b64decode(data)

    def _serialize(self, data):
        if data is None:
            return None
        return base64.b64encode(data).decode()


class GeoPoint(Field):
    name = "geo_point"


class GeoShape(Field):
    name = "geo_shape"


class Completion(Field):
    _param_defs = {
        "analyzer": {"type": "analyzer"},
        "search_analyzer": {"type": "analyzer"},
    }
    name = "completion"


class Percolator(Field):
    name = "percolator"
    _coerce = True

    def _deserialize(self, data):
        return Q(data)

    def _serialize(self, data):
        if data is None:
            return None
        return data.to_dict()


class RangeField(Field):
    _coerce = True
    _core_field = None

    def _deserialize(self, data):
        if isinstance(data, Range):
            return data
        data = dict((k, self._core_field.deserialize(v)) for k, v in iteritems(data))
        return Range(data)

    def _serialize(self, data):
        if data is None:
            return None
        if not isinstance(data, collections_abc.Mapping):
            data = data.to_dict()
        return dict((k, self._core_field.serialize(v)) for k, v in iteritems(data))


class IntegerRange(RangeField):
    name = "integer_range"
    _core_field = Integer()


class FloatRange(RangeField):
    name = "float_range"
    _core_field = Float()


class LongRange(RangeField):
    name = "long_range"
    _core_field = Long()


class DoubleRange(RangeField):
    name = "double_range"
    _core_field = Double()


class DateRange(RangeField):
    name = "date_range"
    _core_field = Date()


class IpRange(Field):
    # not a RangeField since ip_range supports CIDR ranges
    name = "ip_range"


class Join(Field):
    name = "join"


class TokenCount(Field):
    name = "token_count"


class Murmur3(Field):
    name = "murmur3"
