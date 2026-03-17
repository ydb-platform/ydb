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

from typing import Any

from ..utils import AttrDict, AttrList, _wrap
from .hit import Hit, HitMeta


class Response(AttrDict):
    def __init__(self, search: Any, response: Any, doc_class: Any = None) -> None:
        super(AttrDict, self).__setattr__("_search", search)
        super(AttrDict, self).__setattr__("_doc_class", doc_class)
        super().__init__(response)

    def __iter__(self) -> Any:
        return iter(self.hits)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (slice, int)):
            # for slicing etc
            return self.hits[key]
        return super().__getitem__(key)

    def __nonzero__(self) -> Any:
        return bool(self.hits)

    __bool__ = __nonzero__

    def __repr__(self) -> str:
        return "<Response: %r>" % (self.hits or self.aggregations)

    def __len__(self) -> int:
        return len(self.hits)

    def __getstate__(self) -> Any:
        return self._d_, self._search, self._doc_class

    def __setstate__(self, state: Any) -> None:
        super(AttrDict, self).__setattr__("_d_", state[0])
        super(AttrDict, self).__setattr__("_search", state[1])
        super(AttrDict, self).__setattr__("_doc_class", state[2])

    def success(self) -> bool:
        return self._shards.total == self._shards.successful and not self.timed_out

    @property
    def hits(self) -> Any:
        if not hasattr(self, "_hits"):
            h = self._d_["hits"]

            try:
                hits = AttrList(map(self._search._get_result, h["hits"]))
            except AttributeError as e:
                # avoid raising AttributeError since it will be hidden by the property
                raise TypeError("Could not parse hits.", e)

            # avoid assigning _hits into self._d_
            super(AttrDict, self).__setattr__("_hits", hits)
            for k in h:
                setattr(self._hits, k, _wrap(h[k]))
        return self._hits

    @property
    def aggregations(self) -> Any:
        return self.aggs

    @property
    def aggs(self) -> Any:
        if not hasattr(self, "_aggs"):
            aggs = AggResponse(
                self._search.aggs, self._search, self._d_.get("aggregations", {})
            )

            # avoid assigning _aggs into self._d_
            super(AttrDict, self).__setattr__("_aggs", aggs)
        return self._aggs


class AggResponse(AttrDict):
    def __init__(self, aggs: Any, search: Any, data: Any) -> None:
        super(AttrDict, self).__setattr__("_meta", {"search": search, "aggs": aggs})
        super().__init__(data)

    def __getitem__(self, attr_name: Any) -> Any:
        if attr_name in self._meta["aggs"]:
            # don't do self._meta['aggs'][attr_name] to avoid copying
            agg = self._meta["aggs"].aggs[attr_name]
            return agg.result(self._meta["search"], self._d_[attr_name])
        return super().__getitem__(attr_name)

    def __iter__(self) -> Any:
        for name in self._meta["aggs"]:
            yield self[name]


class UpdateByQueryResponse(AttrDict):
    def __init__(self, search: Any, response: Any, doc_class: Any = None) -> None:
        super(AttrDict, self).__setattr__("_search", search)
        super(AttrDict, self).__setattr__("_doc_class", doc_class)
        super().__init__(response)

    def success(self) -> bool:
        return not self.timed_out and not self.failures


__all__ = ["Response", "AggResponse", "UpdateByQueryResponse", "Hit", "HitMeta"]
