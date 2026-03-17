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
from copy import deepcopy
from typing import (
    Any,
    ClassVar,
    Dict,
    Literal,
    MutableMapping,
    Optional,
    Union,
    overload,
)

from elastic_transport.client_utils import DEFAULT, DefaultType

from .utils import AttrDict, DslBase


@overload
def SF(name_or_sf: MutableMapping[str, Any]) -> "ScoreFunction": ...


@overload
def SF(name_or_sf: "ScoreFunction") -> "ScoreFunction": ...


@overload
def SF(name_or_sf: str, **params: Any) -> "ScoreFunction": ...


def SF(
    name_or_sf: Union[str, "ScoreFunction", MutableMapping[str, Any]],
    **params: Any,
) -> "ScoreFunction":
    # {"script_score": {"script": "_score"}, "filter": {}}
    if isinstance(name_or_sf, collections.abc.MutableMapping):
        if params:
            raise ValueError("SF() cannot accept parameters when passing in a dict.")

        kwargs: Dict[str, Any] = {}
        sf = deepcopy(name_or_sf)
        for k in ScoreFunction._param_defs:
            if k in name_or_sf:
                kwargs[k] = sf.pop(k)

        # not sf, so just filter+weight, which used to be boost factor
        sf_params = params
        if not sf:
            name = "boost_factor"
        # {'FUNCTION': {...}}
        elif len(sf) == 1:
            name, sf_params = sf.popitem()
        else:
            raise ValueError(f"SF() got an unexpected fields in the dictionary: {sf!r}")

        # boost factor special case, see elasticsearch #6343
        if not isinstance(sf_params, collections.abc.Mapping):
            sf_params = {"value": sf_params}

        # mix known params (from _param_defs) and from inside the function
        kwargs.update(sf_params)
        return ScoreFunction.get_dsl_class(name)(**kwargs)

    # ScriptScore(script="_score", filter=Q())
    if isinstance(name_or_sf, ScoreFunction):
        if params:
            raise ValueError(
                "SF() cannot accept parameters when passing in a ScoreFunction object."
            )
        return name_or_sf

    # "script_score", script="_score", filter=Q()
    return ScoreFunction.get_dsl_class(name_or_sf)(**params)


class ScoreFunction(DslBase):
    _type_name = "score_function"
    _type_shortcut = staticmethod(SF)
    _param_defs = {
        "query": {"type": "query"},
        "filter": {"type": "query"},
        "weight": {},
    }
    name: ClassVar[Optional[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        # filter and query dicts should be at the same level as us
        for k in self._param_defs:
            if self.name is not None:
                val = d[self.name]
                if isinstance(val, dict) and k in val:
                    d[k] = val.pop(k)
        return d


class ScriptScore(ScoreFunction):
    name = "script_score"


class BoostFactor(ScoreFunction):
    name = "boost_factor"

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if self.name is not None:
            val = d[self.name]
            if isinstance(val, dict):
                if "value" in val:
                    d[self.name] = val.pop("value")
                else:
                    del d[self.name]
        return d


class RandomScore(ScoreFunction):
    name = "random_score"


class FieldValueFactorScore(ScoreFunction):
    name = "field_value_factor"


class FieldValueFactor(FieldValueFactorScore):  # alias of the above
    pass


class Linear(ScoreFunction):
    name = "linear"


class Gauss(ScoreFunction):
    name = "gauss"


class Exp(ScoreFunction):
    name = "exp"


class DecayFunction(AttrDict[Any]):
    def __init__(
        self,
        *,
        decay: Union[float, "DefaultType"] = DEFAULT,
        offset: Any = DEFAULT,
        scale: Any = DEFAULT,
        origin: Any = DEFAULT,
        multi_value_mode: Union[
            Literal["min", "max", "avg", "sum"], "DefaultType"
        ] = DEFAULT,
        **kwargs: Any,
    ):
        if decay != DEFAULT:
            kwargs["decay"] = decay
        if offset != DEFAULT:
            kwargs["offset"] = offset
        if scale != DEFAULT:
            kwargs["scale"] = scale
        if origin != DEFAULT:
            kwargs["origin"] = origin
        if multi_value_mode != DEFAULT:
            kwargs["multi_value_mode"] = multi_value_mode
        super().__init__(kwargs)
