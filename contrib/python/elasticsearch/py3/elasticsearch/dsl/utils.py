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


import collections.abc
from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from elastic_transport.client_utils import DEFAULT
from typing_extensions import Self, TypeAlias, TypeVar

from .exceptions import UnknownDslObject, ValidationException

if TYPE_CHECKING:
    from elastic_transport import ObjectApiResponse

    from elasticsearch import AsyncElasticsearch, Elasticsearch

    from .document_base import DocumentOptions
    from .field import Field
    from .index_base import IndexBase
    from .response import Hit  # noqa: F401
    from .types import Hit as HitBaseType

UsingType: TypeAlias = Union[str, "Elasticsearch"]
AsyncUsingType: TypeAlias = Union[str, "AsyncElasticsearch"]
AnyUsingType: TypeAlias = Union[str, "Elasticsearch", "AsyncElasticsearch"]

_ValT = TypeVar("_ValT")  # used by AttrDict
_R = TypeVar("_R", default="Hit")  # used by Search and Response classes

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


def _wrap(val: Any, obj_wrapper: Optional[Callable[[Any], Any]] = None) -> Any:
    if isinstance(val, dict):
        return AttrDict(val) if obj_wrapper is None else obj_wrapper(val)
    if isinstance(val, list):
        return AttrList(val)
    return val


def _recursive_to_dict(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    elif isinstance(value, dict) or isinstance(value, AttrDict):
        return {k: _recursive_to_dict(v) for k, v in value.items()}
    elif isinstance(value, list) or isinstance(value, AttrList):
        return [recursive_to_dict(elem) for elem in value]
    else:
        return value


class AttrList(Generic[_ValT]):
    def __init__(
        self, l: List[_ValT], obj_wrapper: Optional[Callable[[_ValT], Any]] = None
    ):
        # make iterables into lists
        if not isinstance(l, list):
            l = list(l)
        self._l_ = l
        self._obj_wrapper = obj_wrapper

    def __repr__(self) -> str:
        return repr(self._l_)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AttrList):
            return other._l_ == self._l_
        # make sure we still equal to a dict with the same data
        return bool(other == self._l_)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __getitem__(self, k: Union[int, slice]) -> Any:
        l = self._l_[k]
        if isinstance(k, slice):
            return AttrList[_ValT](l, obj_wrapper=self._obj_wrapper)  # type: ignore[arg-type]
        return _wrap(l, self._obj_wrapper)

    def __setitem__(self, k: int, value: _ValT) -> None:
        self._l_[k] = value

    def __iter__(self) -> Iterator[Any]:
        return map(lambda i: _wrap(i, self._obj_wrapper), self._l_)

    def __len__(self) -> int:
        return len(self._l_)

    def __nonzero__(self) -> bool:
        return bool(self._l_)

    __bool__ = __nonzero__

    def __getattr__(self, name: str) -> Any:
        return getattr(self._l_, name)

    def __getstate__(self) -> Tuple[List[_ValT], Optional[Callable[[_ValT], Any]]]:
        return self._l_, self._obj_wrapper

    def __setstate__(
        self, state: Tuple[List[_ValT], Optional[Callable[[_ValT], Any]]]
    ) -> None:
        self._l_, self._obj_wrapper = state

    def to_list(self) -> List[_ValT]:
        return self._l_


class AttrDict(Generic[_ValT]):
    """
    Helper class to provide attribute like access (read and write) to
    dictionaries. Used to provide a convenient way to access both results and
    nested dsl dicts.
    """

    _d_: Dict[str, _ValT]
    RESERVED: Dict[str, str] = {"from_": "from"}

    def __init__(self, d: Dict[str, _ValT]):
        # assign the inner dict manually to prevent __setattr__ from firing
        super().__setattr__("_d_", d)

    def __contains__(self, key: object) -> bool:
        return key in self._d_

    def __nonzero__(self) -> bool:
        return bool(self._d_)

    __bool__ = __nonzero__

    def __dir__(self) -> List[str]:
        # introspection for auto-complete in IPython etc
        return list(self._d_.keys())

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AttrDict):
            return other._d_ == self._d_
        # make sure we still equal to a dict with the same data
        return bool(other == self._d_)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __repr__(self) -> str:
        r = repr(self._d_)
        if len(r) > 60:
            r = r[:60] + "...}"
        return r

    def __getstate__(self) -> Tuple[Dict[str, _ValT]]:
        return (self._d_,)

    def __setstate__(self, state: Tuple[Dict[str, _ValT]]) -> None:
        super().__setattr__("_d_", state[0])

    def __getattr__(self, attr_name: str) -> Any:
        try:
            return self.__getitem__(attr_name)
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute {attr_name!r}"
            )

    def __delattr__(self, attr_name: str) -> None:
        try:
            del self._d_[self.RESERVED.get(attr_name, attr_name)]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute {attr_name!r}"
            )

    def __getitem__(self, key: str) -> Any:
        return _wrap(self._d_[self.RESERVED.get(key, key)])

    def __setitem__(self, key: str, value: _ValT) -> None:
        self._d_[self.RESERVED.get(key, key)] = value

    def __delitem__(self, key: str) -> None:
        del self._d_[self.RESERVED.get(key, key)]

    def __setattr__(self, name: str, value: _ValT) -> None:
        # the __orig__class__ attribute has to be treated as an exception, as
        # is it added to an object when it is instantiated with type arguments
        if (
            name in self._d_ or not hasattr(self.__class__, name)
        ) and name != "__orig_class__":
            self._d_[self.RESERVED.get(name, name)] = value
        else:
            # there is an attribute on the class (could be property, ..) - don't add it as field
            super().__setattr__(name, value)

    def __iter__(self) -> Iterator[str]:
        return iter(self._d_)

    def to_dict(self, recursive: bool = False) -> Dict[str, _ValT]:
        return cast(
            Dict[str, _ValT], _recursive_to_dict(self._d_) if recursive else self._d_
        )

    def keys(self) -> Iterable[str]:
        return self._d_.keys()

    def items(self) -> Iterable[Tuple[str, _ValT]]:
        return self._d_.items()


class DslMeta(type):
    """
    Base Metaclass for DslBase subclasses that builds a registry of all classes
    for given DslBase subclass (== all the query types for the Query subclass
    of DslBase).

    It then uses the information from that registry (as well as `name` and
    `shortcut` attributes from the base class) to construct any subclass based
    on it's name.

    For typical use see `QueryMeta` and `Query` in `elasticsearch.dsl.query`.
    """

    name: str
    _classes: Dict[str, type]
    _type_name: str
    _types: ClassVar[Dict[str, Type["DslBase"]]] = {}

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        super().__init__(name, bases, attrs)
        # skip for DslBase
        if not hasattr(cls, "_type_shortcut"):
            return
        if not cls.name:
            # abstract base class, register it's shortcut
            cls._types[cls._type_name] = cls._type_shortcut
            # and create a registry for subclasses
            if not hasattr(cls, "_classes"):
                cls._classes = {}
        elif cls.name not in cls._classes:
            # normal class, register it
            cls._classes[cls.name] = cls

    @classmethod
    def get_dsl_type(cls, name: str) -> Type["DslBase"]:
        try:
            return cls._types[name]
        except KeyError:
            raise UnknownDslObject(f"DSL type {name} does not exist.")


class DslBase(metaclass=DslMeta):
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

    _param_defs: ClassVar[Dict[str, Dict[str, Union[str, bool]]]] = {}

    @classmethod
    def get_dsl_class(
        cls: Type[Self], name: str, default: Optional[str] = None
    ) -> Type[Self]:
        try:
            return cls._classes[name]
        except KeyError:
            if default is not None:
                return cls._classes[default]
            raise UnknownDslObject(
                f"DSL class `{name}` does not exist in {cls._type_name}."
            )

    def __init__(self, _expand__to_dot: Optional[bool] = None, **params: Any) -> None:
        if _expand__to_dot is None:
            _expand__to_dot = EXPAND__TO_DOT
        self._params: Dict[str, Any] = {}
        for pname, pvalue in params.items():
            if pvalue is DEFAULT:
                continue
            # expand "__" to dots
            if "__" in pname and _expand__to_dot:
                pname = pname.replace("__", ".")
            # convert instrumented fields to string
            if type(pvalue).__name__ == "InstrumentedField":
                pvalue = str(pvalue)
            self._setattr(pname, pvalue)

    def _repr_params(self) -> str:
        """Produce a repr of all our parameters to be used in __repr__."""
        return ", ".join(
            f"{n.replace('.', '__')}={v!r}"
            for (n, v) in sorted(self._params.items())
            # make sure we don't include empty typed params
            if "type" not in self._param_defs.get(n, {}) or v
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._repr_params()})"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and other.to_dict() == self.to_dict()

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            return super().__setattr__(name, value)
        return self._setattr(name, value)

    def _setattr(self, name: str, value: Any) -> None:
        # if this attribute has special type assigned to it...
        name = AttrDict.RESERVED.get(name, name)
        if name in self._param_defs:
            pinfo = self._param_defs[name]

            if "type" in pinfo:
                # get the shortcut used to construct this type (query.Q, aggs.A, etc)
                shortcut = self.__class__.get_dsl_type(str(pinfo["type"]))

                # list of dict(name -> DslBase)
                if pinfo.get("multi") and pinfo.get("hash"):
                    if not isinstance(value, (tuple, list)):
                        value = (value,)
                    value = list(
                        {k: shortcut(v) for (k, v) in obj.items()} for obj in value
                    )
                elif pinfo.get("multi"):
                    if not isinstance(value, (tuple, list)):
                        value = (value,)
                    value = list(map(shortcut, value))

                # dict(name -> DslBase), make sure we pickup all the objs
                elif pinfo.get("hash"):
                    value = {k: shortcut(v) for (k, v) in value.items()}

                # single value object, just convert
                else:
                    value = shortcut(value)
        self._params[name] = value

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute {name!r}"
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
                f"{self.__class__.__name__!r} object has no attribute {name!r}"
            )

        # wrap nested dicts in AttrDict for convenient access
        if isinstance(value, dict):
            return AttrDict(value)
        return value

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the DSL object to plain dict
        """
        d = {}
        for pname, value in self._params.items():
            pinfo = self._param_defs.get(pname)

            # typed param
            if pinfo and "type" in pinfo:
                # don't serialize empty lists and dicts for typed fields
                if value in ({}, []):
                    continue

                # list of dict(name -> DslBase)
                if pinfo.get("multi") and pinfo.get("hash"):
                    value = list(
                        {k: v.to_dict() for k, v in obj.items()} for obj in value
                    )

                # multi-values are serialized as list of dicts
                elif pinfo.get("multi"):
                    value = list(map(lambda x: x.to_dict(), value))

                # squash all the hash values into one dict
                elif pinfo.get("hash"):
                    value = {k: v.to_dict() for k, v in value.items()}

                # serialize single values
                else:
                    value = value.to_dict()

            # serialize anything with to_dict method
            elif hasattr(value, "to_dict"):
                value = value.to_dict()

            d[pname] = value
        return {self.name: d}

    def _clone(self) -> Self:
        c = self.__class__()
        for attr in self._params:
            c._params[attr] = copy(self._params[attr])
        return c


if TYPE_CHECKING:
    HitMetaBase = HitBaseType
else:
    HitMetaBase = AttrDict[Any]


class HitMeta(HitMetaBase):
    inner_hits: Mapping[str, Any]

    def __init__(
        self,
        document: Dict[str, Any],
        exclude: Tuple[str, ...] = ("_source", "_fields"),
    ):
        d = {
            k[1:] if k.startswith("_") else k: v
            for (k, v) in document.items()
            if k not in exclude
        }
        if "type" in d:
            # make sure we are consistent everywhere in python
            d["doc_type"] = d.pop("type")
        super().__init__(d)


class ObjectBase(AttrDict[Any]):
    _doc_type: "DocumentOptions"
    _index: "IndexBase"
    meta: HitMeta

    def __init__(self, meta: Optional[Dict[str, Any]] = None, **kwargs: Any):
        meta = meta or {}
        for k in list(kwargs):
            if k.startswith("_") and k[1:] in META_FIELDS:
                meta[k] = kwargs.pop(k)

        super(AttrDict, self).__setattr__("meta", HitMeta(meta))

        # process field defaults
        if hasattr(self, "_defaults"):
            for name in self._defaults:
                if name not in kwargs:
                    value = self._defaults[name]
                    if callable(value):
                        value = value()
                    kwargs[name] = value

        super().__init__(kwargs)

    @classmethod
    def __list_fields(cls) -> Iterator[Tuple[str, "Field", bool]]:
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
    def __get_field(cls, name: str) -> Optional["Field"]:
        try:
            return cls._doc_type.mapping[name]
        except KeyError:
            # fallback to fields on the Index
            if hasattr(cls, "_index") and cls._index._mapping:
                try:
                    return cls._index._mapping[name]
                except KeyError:
                    pass
            return None

    @classmethod
    def from_es(cls, hit: Union[Dict[str, Any], "ObjectApiResponse[Any]"]) -> Self:
        meta = hit.copy()
        data = meta.pop("_source", {})
        doc = cls(meta=meta)
        doc._from_dict(data)
        return doc

    def _from_dict(self, data: Dict[str, Any]) -> None:
        for k, v in data.items():
            f = self.__get_field(k)
            if f and f._coerce:
                v = f.deserialize(v)
            setattr(self, k, v)

    def __getstate__(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:  # type: ignore[override]
        return self.to_dict(), self.meta._d_

    def __setstate__(self, state: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:  # type: ignore[override]
        data, meta = state
        super(AttrDict, self).__setattr__("_d_", {})
        super(AttrDict, self).__setattr__("meta", HitMeta(meta))
        self._from_dict(data)

    def __getattr__(self, name: str) -> Any:
        try:
            return super().__getattr__(name)
        except AttributeError:
            f = self.__get_field(name)
            if f is not None and hasattr(f, "empty"):
                value = f.empty()
                if value not in SKIP_VALUES:
                    setattr(self, name, value)
                    value = getattr(self, name)
                return value
            raise

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__class__._doc_type.mapping:
            self._d_[name] = value
        else:
            super().__setattr__(name, value)

    def to_dict(self, skip_empty: bool = True) -> Dict[str, Any]:
        out = {}
        for k, v in self._d_.items():
            # if this is a mapped field,
            f = self.__get_field(k)
            if f and f._coerce:
                v = f.serialize(v, skip_empty=skip_empty)

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

    def clean_fields(self, validate: bool = True) -> None:
        errors: Dict[str, List[ValidationException]] = {}
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
                self._d_[name] = cast(Any, data)

        if validate and errors:
            raise ValidationException(errors)

    def clean(self) -> None:
        pass

    def full_clean(self) -> None:
        self.clean_fields(validate=False)
        self.clean()
        self.clean_fields(validate=True)


def merge(
    data: Union[Dict[str, Any], AttrDict[Any]],
    new_data: Union[Dict[str, Any], AttrDict[Any]],
    raise_on_conflict: bool = False,
) -> None:
    if not (
        isinstance(data, (AttrDict, collections.abc.Mapping))
        and isinstance(new_data, (AttrDict, collections.abc.Mapping))
    ):
        raise ValueError(
            f"You can only merge two dicts! Got {data!r} and {new_data!r} instead."
        )

    for key, value in new_data.items():
        if (
            key in data
            and isinstance(data[key], (AttrDict, collections.abc.Mapping))
            and isinstance(value, (AttrDict, collections.abc.Mapping))
        ):
            merge(data[key], value, raise_on_conflict)  # type: ignore[arg-type]
        elif key in data and data[key] != value and raise_on_conflict:
            raise ValueError(f"Incompatible data for key {key!r}, cannot be merged.")
        else:
            data[key] = value


def recursive_to_dict(data: Any) -> Any:
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
    elif isinstance(data, dict):
        return {key: recursive_to_dict(val) for key, val in data.items()}
    return data
