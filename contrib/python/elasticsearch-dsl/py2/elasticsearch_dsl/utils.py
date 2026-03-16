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

from __future__ import unicode_literals

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:
    import collections as collections_abc

from copy import copy

from six import add_metaclass, iteritems
from six.moves import map

from .exceptions import UnknownDslObject, ValidationException

SKIP_VALUES = ("", None)
EXPAND__TO_DOT = True

DOC_META_FIELDS = frozenset(
    (
        "id",
        "routing",
    )
)

META_FIELDS = frozenset(
    (
        # Elasticsearch metadata fields, except 'type'
        "index",
        "using",
        "score",
        "version",
        "seq_no",
        "primary_term",
    )
).union(DOC_META_FIELDS)


def _wrap(val, obj_wrapper=None):
    if isinstance(val, collections_abc.Mapping):
        return AttrDict(val) if obj_wrapper is None else obj_wrapper(val)
    if isinstance(val, list):
        return AttrList(val)
    return val


class AttrList(object):
    def __init__(self, l, obj_wrapper=None):
        # make iterables into lists
        if not isinstance(l, list):
            l = list(l)
        self._l_ = l
        self._obj_wrapper = obj_wrapper

    def __repr__(self):
        return repr(self._l_)

    def __eq__(self, other):
        if isinstance(other, AttrList):
            return other._l_ == self._l_
        # make sure we still equal to a dict with the same data
        return other == self._l_

    def __ne__(self, other):
        return not self == other

    def __getitem__(self, k):
        l = self._l_[k]
        if isinstance(k, slice):
            return AttrList(l, obj_wrapper=self._obj_wrapper)
        return _wrap(l, self._obj_wrapper)

    def __setitem__(self, k, value):
        self._l_[k] = value

    def __iter__(self):
        return map(lambda i: _wrap(i, self._obj_wrapper), self._l_)

    def __len__(self):
        return len(self._l_)

    def __nonzero__(self):
        return bool(self._l_)

    __bool__ = __nonzero__

    def __getattr__(self, name):
        return getattr(self._l_, name)

    def __getstate__(self):
        return self._l_, self._obj_wrapper

    def __setstate__(self, state):
        self._l_, self._obj_wrapper = state


class AttrDict(object):
    """
    Helper class to provide attribute like access (read and write) to
    dictionaries. Used to provide a convenient way to access both results and
    nested dsl dicts.
    """

    def __init__(self, d):
        # assign the inner dict manually to prevent __setattr__ from firing
        super(AttrDict, self).__setattr__("_d_", d)

    def __contains__(self, key):
        return key in self._d_

    def __nonzero__(self):
        return bool(self._d_)

    __bool__ = __nonzero__

    def __dir__(self):
        # introspection for auto-complete in IPython etc
        return list(self._d_.keys())

    def __eq__(self, other):
        if isinstance(other, AttrDict):
            return other._d_ == self._d_
        # make sure we still equal to a dict with the same data
        return other == self._d_

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        r = repr(self._d_)
        if len(r) > 60:
            r = r[:60] + "...}"
        return r

    def __getstate__(self):
        return (self._d_,)

    def __setstate__(self, state):
        super(AttrDict, self).__setattr__("_d_", state[0])

    def __getattr__(self, attr_name):
        try:
            return self.__getitem__(attr_name)
        except KeyError:
            raise AttributeError(
                "{!r} object has no attribute {!r}".format(
                    self.__class__.__name__, attr_name
                )
            )

    def __delattr__(self, attr_name):
        try:
            del self._d_[attr_name]
        except KeyError:
            raise AttributeError(
                "{!r} object has no attribute {!r}".format(
                    self.__class__.__name__, attr_name
                )
            )

    def __getitem__(self, key):
        return _wrap(self._d_[key])

    def __setitem__(self, key, value):
        self._d_[key] = value

    def __delitem__(self, key):
        del self._d_[key]

    def __setattr__(self, name, value):
        if name in self._d_ or not hasattr(self.__class__, name):
            self._d_[name] = value
        else:
            # there is an attribute on the class (could be property, ..) - don't add it as field
            super(AttrDict, self).__setattr__(name, value)

    def __iter__(self):
        return iter(self._d_)

    def to_dict(self):
        return self._d_


class DslMeta(type):
    """
    Base Metaclass for DslBase subclasses that builds a registry of all classes
    for given DslBase subclass (== all the query types for the Query subclass
    of DslBase).

    It then uses the information from that registry (as well as `name` and
    `shortcut` attributes from the base class) to construct any subclass based
    on it's name.

    For typical use see `QueryMeta` and `Query` in `elasticsearch_dsl.query`.
    """

    _types = {}

    def __init__(cls, name, bases, attrs):
        super(DslMeta, cls).__init__(name, bases, attrs)
        # skip for DslBase
        if not hasattr(cls, "_type_shortcut"):
            return
        if cls.name is None:
            # abstract base class, register it's shortcut
            cls._types[cls._type_name] = cls._type_shortcut
            # and create a registry for subclasses
            if not hasattr(cls, "_classes"):
                cls._classes = {}
        elif cls.name not in cls._classes:
            # normal class, register it
            cls._classes[cls.name] = cls

    @classmethod
    def get_dsl_type(cls, name):
        try:
            return cls._types[name]
        except KeyError:
            raise UnknownDslObject("DSL type %s does not exist." % name)


@add_metaclass(DslMeta)
class DslBase(object):
    """
    Base class for all DSL objects - queries, filters, aggregations etc. Wraps
    a dictionary representing the object's json.

    Provides several feature:
        - attribute access to the wrapped dictionary (.field instead of ['field'])
        - _clone method returning a copy of self
        - to_dict method to serialize into dict (to be sent via elasticsearch-py)
        - basic logical operators (&, | and ~) using a Bool(Filter|Query) TODO:
          move into a class specific for Query/Filter
        - respects the definition of the class and (de)serializes it's
          attributes based on the `_param_defs` definition (for example turning
          all values in the `must` attribute into Query objects)
    """

    _param_defs = {}

    @classmethod
    def get_dsl_class(cls, name, default=None):
        try:
            return cls._classes[name]
        except KeyError:
            if default is not None:
                return cls._classes[default]
            raise UnknownDslObject(
                "DSL class `{}` does not exist in {}.".format(name, cls._type_name)
            )

    def __init__(self, _expand__to_dot=EXPAND__TO_DOT, **params):
        self._params = {}
        for pname, pvalue in iteritems(params):
            if "__" in pname and _expand__to_dot:
                pname = pname.replace("__", ".")
            self._setattr(pname, pvalue)

    def _repr_params(self):
        """Produce a repr of all our parameters to be used in __repr__."""
        return ", ".join(
            "{}={!r}".format(n.replace(".", "__"), v)
            for (n, v) in sorted(iteritems(self._params))
            # make sure we don't include empty typed params
            if "type" not in self._param_defs.get(n, {}) or v
        )

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._repr_params())

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.to_dict() == self.to_dict()

    def __ne__(self, other):
        return not self == other

    def __setattr__(self, name, value):
        if name.startswith("_"):
            return super(DslBase, self).__setattr__(name, value)
        return self._setattr(name, value)

    def _setattr(self, name, value):
        # if this attribute has special type assigned to it...
        if name in self._param_defs:
            pinfo = self._param_defs[name]

            if "type" in pinfo:
                # get the shortcut used to construct this type (query.Q, aggs.A, etc)
                shortcut = self.__class__.get_dsl_type(pinfo["type"])

                # list of dict(name -> DslBase)
                if pinfo.get("multi") and pinfo.get("hash"):
                    if not isinstance(value, (tuple, list)):
                        value = (value,)
                    value = list(
                        {k: shortcut(v) for (k, v) in iteritems(obj)} for obj in value
                    )
                elif pinfo.get("multi"):
                    if not isinstance(value, (tuple, list)):
                        value = (value,)
                    value = list(map(shortcut, value))

                # dict(name -> DslBase), make sure we pickup all the objs
                elif pinfo.get("hash"):
                    value = {k: shortcut(v) for (k, v) in iteritems(value)}

                # single value object, just convert
                else:
                    value = shortcut(value)
        self._params[name] = value

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(
                "{!r} object has no attribute {!r}".format(
                    self.__class__.__name__, name
                )
            )

        value = None
        try:
            value = self._params[name]
        except KeyError:
            # compound types should never throw AttributeError and return empty
            # container instead
            if name in self._param_defs:
                pinfo = self._param_defs[name]
                if pinfo.get("multi"):
                    value = self._params.setdefault(name, [])
                elif pinfo.get("hash"):
                    value = self._params.setdefault(name, {})
        if value is None:
            raise AttributeError(
                "{!r} object has no attribute {!r}".format(
                    self.__class__.__name__, name
                )
            )

        # wrap nested dicts in AttrDict for convenient access
        if isinstance(value, collections_abc.Mapping):
            return AttrDict(value)
        return value

    def to_dict(self):
        """
        Serialize the DSL object to plain dict
        """
        d = {}
        for pname, value in iteritems(self._params):
            pinfo = self._param_defs.get(pname)

            # typed param
            if pinfo and "type" in pinfo:
                # don't serialize empty lists and dicts for typed fields
                if value in ({}, []):
                    continue

                # list of dict(name -> DslBase)
                if pinfo.get("multi") and pinfo.get("hash"):
                    value = list(
                        {k: v.to_dict() for k, v in iteritems(obj)} for obj in value
                    )

                # multi-values are serialized as list of dicts
                elif pinfo.get("multi"):
                    value = list(map(lambda x: x.to_dict(), value))

                # squash all the hash values into one dict
                elif pinfo.get("hash"):
                    value = {k: v.to_dict() for k, v in iteritems(value)}

                # serialize single values
                else:
                    value = value.to_dict()

            # serialize anything with to_dict method
            elif hasattr(value, "to_dict"):
                value = value.to_dict()

            d[pname] = value
        return {self.name: d}

    def _clone(self):
        c = self.__class__()
        for attr in self._params:
            c._params[attr] = copy(self._params[attr])
        return c


class HitMeta(AttrDict):
    def __init__(self, document, exclude=("_source", "_fields")):
        d = {
            k[1:] if k.startswith("_") else k: v
            for (k, v) in iteritems(document)
            if k not in exclude
        }
        if "type" in d:
            # make sure we are consistent everywhere in python
            d["doc_type"] = d.pop("type")
        super(HitMeta, self).__init__(d)


class ObjectBase(AttrDict):
    def __init__(self, meta=None, **kwargs):
        meta = meta or {}
        for k in list(kwargs):
            if k.startswith("_") and k[1:] in META_FIELDS:
                meta[k] = kwargs.pop(k)

        super(AttrDict, self).__setattr__("meta", HitMeta(meta))

        super(ObjectBase, self).__init__(kwargs)

    @classmethod
    def __list_fields(cls):
        """
        Get all the fields defined for our class, if we have an Index, try
        looking at the index mappings as well, mark the fields from Index as
        optional.
        """
        for name in cls._doc_type.mapping:
            field = cls._doc_type.mapping[name]
            yield name, field, False

        if hasattr(cls.__class__, "_index"):
            if not cls._index._mapping:
                return
            for name in cls._index._mapping:
                # don't return fields that are in _doc_type
                if name in cls._doc_type.mapping:
                    continue
                field = cls._index._mapping[name]
                yield name, field, True

    @classmethod
    def __get_field(cls, name):
        try:
            return cls._doc_type.mapping[name]
        except KeyError:
            # fallback to fields on the Index
            if hasattr(cls, "_index") and cls._index._mapping:
                try:
                    return cls._index._mapping[name]
                except KeyError:
                    pass

    @classmethod
    def from_es(cls, hit):
        meta = hit.copy()
        data = meta.pop("_source", {})
        doc = cls(meta=meta)
        doc._from_dict(data)
        return doc

    def _from_dict(self, data):
        for k, v in iteritems(data):
            f = self.__get_field(k)
            if f and f._coerce:
                v = f.deserialize(v)
            setattr(self, k, v)

    def __getstate__(self):
        return self.to_dict(), self.meta._d_

    def __setstate__(self, state):
        data, meta = state
        super(AttrDict, self).__setattr__("_d_", {})
        super(AttrDict, self).__setattr__("meta", HitMeta(meta))
        self._from_dict(data)

    def __getattr__(self, name):
        try:
            return super(ObjectBase, self).__getattr__(name)
        except AttributeError:
            f = self.__get_field(name)
            if hasattr(f, "empty"):
                value = f.empty()
                if value not in SKIP_VALUES:
                    setattr(self, name, value)
                    value = getattr(self, name)
                return value
            raise

    def to_dict(self, skip_empty=True):
        out = {}
        for k, v in iteritems(self._d_):
            # if this is a mapped field,
            f = self.__get_field(k)
            if f and f._coerce:
                v = f.serialize(v)

            # if someone assigned AttrList, unwrap it
            if isinstance(v, AttrList):
                v = v._l_

            if skip_empty:
                # don't serialize empty values
                # careful not to include numeric zeros
                if v in ([], {}, None):
                    continue

            out[k] = v
        return out

    def clean_fields(self):
        errors = {}
        for name, field, optional in self.__list_fields():
            data = self._d_.get(name, None)
            if data is None and optional:
                continue
            try:
                # save the cleaned value
                data = field.clean(data)
            except ValidationException as e:
                errors.setdefault(name, []).append(e)

            if name in self._d_ or data not in ([], {}, None):
                self._d_[name] = data

        if errors:
            raise ValidationException(errors)

    def clean(self):
        pass

    def full_clean(self):
        self.clean_fields()
        self.clean()


def merge(data, new_data, raise_on_conflict=False):
    if not (
        isinstance(data, (AttrDict, collections_abc.Mapping))
        and isinstance(new_data, (AttrDict, collections_abc.Mapping))
    ):
        raise ValueError(
            "You can only merge two dicts! Got {!r} and {!r} instead.".format(
                data, new_data
            )
        )

    for key, value in iteritems(new_data):
        if (
            key in data
            and isinstance(data[key], (AttrDict, collections_abc.Mapping))
            and isinstance(value, (AttrDict, collections_abc.Mapping))
        ):
            merge(data[key], value, raise_on_conflict)
        elif key in data and data[key] != value and raise_on_conflict:
            raise ValueError("Incompatible data for key %r, cannot be merged." % key)
        else:
            data[key] = value


def recursive_to_dict(data):
    """Recursively transform objects that potentially have .to_dict()
    into dictionary literals by traversing AttrList, AttrDict, list,
    tuple, and Mapping types.
    """
    if isinstance(data, AttrList):
        data = list(data._l_)
    elif hasattr(data, "to_dict"):
        data = data.to_dict()
    if isinstance(data, (list, tuple)):
        return type(data)(recursive_to_dict(inner) for inner in data)
    elif isinstance(data, collections_abc.Mapping):
        return {key: recursive_to_dict(val) for key, val in data.items()}
    return data
