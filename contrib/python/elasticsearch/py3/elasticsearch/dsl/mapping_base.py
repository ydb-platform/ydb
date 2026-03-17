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
from itertools import chain
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from typing_extensions import Self

from .field import Field, Nested, Text, construct_field
from .utils import DslBase

META_FIELDS = frozenset(
    (
        "dynamic",
        "transform",
        "dynamic_date_formats",
        "date_detection",
        "numeric_detection",
        "dynamic_templates",
        "enabled",
    )
)


class Properties(DslBase):
    name = "properties"
    _param_defs = {"properties": {"type": "field", "hash": True}}

    properties: Dict[str, Field]

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return "Properties()"

    def __getitem__(self, name: str) -> Field:
        return self.properties[name]

    def __contains__(self, name: str) -> bool:
        return name in self.properties

    def to_dict(self) -> Dict[str, Any]:
        return cast(Dict[str, Field], super().to_dict()["properties"])

    def field(self, name: str, *args: Any, **kwargs: Any) -> Self:
        self.properties[name] = construct_field(*args, **kwargs)
        return self

    def _collect_fields(self) -> Iterator[Field]:
        """Iterate over all Field objects within, including multi fields."""
        fields = cast(Dict[str, Field], self.properties.to_dict())  # type: ignore[attr-defined]
        for f in fields.values():
            yield f
            # multi fields
            if hasattr(f, "fields"):
                yield from f.fields.to_dict().values()
            # nested and inner objects
            if hasattr(f, "_collect_fields"):
                yield from f._collect_fields()

    def update(self, other_object: Any) -> None:
        if not hasattr(other_object, "properties"):
            # not an inner/nested object, no merge possible
            return

        our, other = self.properties, other_object.properties
        for name in other:
            if name in our:
                if hasattr(our[name], "update"):
                    our[name].update(other[name])
                continue
            our[name] = other[name]


class MappingBase:
    def __init__(self) -> None:
        self.properties = Properties()
        self._meta: Dict[str, Any] = {}

    def __repr__(self) -> str:
        return "Mapping()"

    def _clone(self) -> Self:
        m = self.__class__()
        m.properties._params = self.properties._params.copy()
        return m

    def resolve_nested(
        self, field_path: str
    ) -> Tuple[List[str], Optional["MappingBase"]]:
        field = self
        nested = []
        parts = field_path.split(".")
        for i, step in enumerate(parts):
            try:
                field = field[step]  # type: ignore[assignment]
            except KeyError:
                return [], None
            if isinstance(field, Nested):
                nested.append(".".join(parts[: i + 1]))
        return nested, field

    def resolve_field(self, field_path: str) -> Optional[Field]:
        field = self
        for step in field_path.split("."):
            try:
                field = field[step]  # type: ignore[assignment]
            except KeyError:
                return None
        return cast(Field, field)

    def _collect_analysis(self) -> Dict[str, Any]:
        analysis: Dict[str, Any] = {}
        fields = []
        if "_all" in self._meta:
            fields.append(Text(**self._meta["_all"]))

        for f in chain(fields, self.properties._collect_fields()):
            for analyzer_name in (
                "analyzer",
                "normalizer",
                "search_analyzer",
                "search_quote_analyzer",
            ):
                if not hasattr(f, analyzer_name):
                    continue
                analyzer = getattr(f, analyzer_name)
                d = analyzer.get_analysis_definition()
                # empty custom analyzer, probably already defined out of our control
                if not d:
                    continue

                # merge the definition
                # TODO: conflict detection/resolution
                for key in d:
                    analysis.setdefault(key, {}).update(d[key])

        return analysis

    def _update_from_dict(self, raw: Dict[str, Any]) -> None:
        for name, definition in raw.get("properties", {}).items():
            self.field(name, definition)

        # metadata like _all etc
        for name, value in raw.items():
            if name != "properties":
                if isinstance(value, collections.abc.Mapping):
                    self.meta(name, **value)
                else:
                    self.meta(name, value)

    def update(self, mapping: "MappingBase", update_only: bool = False) -> None:
        for name in mapping:
            if update_only and name in self:
                # nested and inner objects, merge recursively
                if hasattr(self[name], "update"):
                    # FIXME only merge subfields, not the settings
                    self[name].update(mapping[name], update_only)
                continue
            self.field(name, mapping[name])

        if update_only:
            for name in mapping._meta:
                if name not in self._meta:
                    self._meta[name] = mapping._meta[name]
        else:
            self._meta.update(mapping._meta)

    def __contains__(self, name: str) -> bool:
        return name in self.properties.properties

    def __getitem__(self, name: str) -> Field:
        return self.properties.properties[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self.properties.properties)

    def field(self, *args: Any, **kwargs: Any) -> Self:
        self.properties.field(*args, **kwargs)
        return self

    def meta(self, name: str, params: Any = None, **kwargs: Any) -> Self:
        if not name.startswith("_") and name not in META_FIELDS:
            name = "_" + name

        if params and kwargs:
            raise ValueError("Meta configs cannot have both value and a dictionary.")

        self._meta[name] = kwargs if params is None else params
        return self

    def to_dict(self) -> Dict[str, Any]:
        meta = self._meta

        # hard coded serialization of analyzers in _all
        if "_all" in meta:
            meta = meta.copy()
            _all = meta["_all"] = meta["_all"].copy()
            for f in ("analyzer", "search_analyzer", "search_quote_analyzer"):
                if hasattr(_all.get(f, None), "to_dict"):
                    _all[f] = _all[f].to_dict()
        meta.update(self.properties.to_dict())
        return meta
