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

import collections.abc as collections_abc
from typing import Any, Optional

from .utils import DslBase


def SF(name_or_sf: Any, **params: Any) -> Any:  # pylint: disable=invalid-name
    # {"script_score": {"script": "_score"}, "filter": {}}
    if isinstance(name_or_sf, collections_abc.Mapping):
        if params:
            raise ValueError("SF() cannot accept parameters when passing in a dict.")
        kwargs = {}
        sf = name_or_sf.copy()  # type: ignore
        for k in ScoreFunction._param_defs:
            if k in name_or_sf:
                kwargs[k] = sf.pop(k)

        # not sf, so just filter+weight, which used to be boost factor
        if not sf:
            name = "boost_factor"
        # {'FUNCTION': {...}}
        elif len(sf) == 1:
            name, params = sf.popitem()
        else:
            raise ValueError(f"SF() got an unexpected fields in the dictionary: {sf!r}")

        # boost factor special case, see https://github.com/elastic/elasticsearch/issues/6343
        if not isinstance(params, collections_abc.Mapping):
            params = {"value": params}

        # mix known params (from _param_defs) and from inside the function
        kwargs.update(params)
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
    _type_name: str = "score_function"
    _type_shortcut = staticmethod(SF)
    _param_defs = {
        "query": {"type": "query"},
        "filter": {"type": "query"},
        "weight": {},
    }
    name: Optional[str] = None

    def to_dict(self) -> Any:
        d = super().to_dict()
        # filter and query dicts should be at the same level as us
        for k in self._param_defs:
            if k in d[self.name]:
                d[k] = d[self.name].pop(k)
        return d


class ScriptScore(ScoreFunction):
    name = "script_score"


class BoostFactor(ScoreFunction):
    name = "boost_factor"

    def to_dict(self) -> Any:
        d = super().to_dict()
        if "value" in d[self.name]:
            d[self.name] = d[self.name].pop("value")
        else:
            del d[self.name]
        return d


class RandomScore(ScoreFunction):
    name = "random_score"


class FieldValueFactor(ScoreFunction):
    name = "field_value_factor"


class Linear(ScoreFunction):
    name = "linear"


class Gauss(ScoreFunction):
    name = "gauss"


class Exp(ScoreFunction):
    name = "exp"
