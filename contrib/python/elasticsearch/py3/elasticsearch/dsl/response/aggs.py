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

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union, cast

from ..utils import _R, AttrDict, AttrList
from . import AggResponse, Response

if TYPE_CHECKING:
    from ..aggs import Agg
    from ..field import Field
    from ..search_base import SearchBase


class Bucket(AggResponse[_R]):
    def __init__(
        self,
        aggs: "Agg[_R]",
        search: "SearchBase[_R]",
        data: Dict[str, Any],
        field: Optional["Field"] = None,
    ):
        super().__init__(aggs, search, data)


class FieldBucket(Bucket[_R]):
    def __init__(
        self,
        aggs: "Agg[_R]",
        search: "SearchBase[_R]",
        data: Dict[str, Any],
        field: Optional["Field"] = None,
    ):
        if field:
            data["key"] = field.deserialize(data["key"])
        super().__init__(aggs, search, data, field)


class BucketData(AggResponse[_R]):
    _bucket_class = Bucket
    _buckets: Union[AttrDict[Any], AttrList[Any]]

    def _wrap_bucket(self, data: Dict[str, Any]) -> Bucket[_R]:
        return self._bucket_class(
            self._meta["aggs"],
            self._meta["search"],
            data,
            field=self._meta.get("field"),
        )

    def __iter__(self) -> Iterator["Agg"]:  # type: ignore[override]
        return iter(self.buckets)

    def __len__(self) -> int:
        return len(self.buckets)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (int, slice)):
            return cast(AttrList[Any], self.buckets)[key]
        return super().__getitem__(key)

    @property
    def buckets(self) -> Union[AttrDict[Any], AttrList[Any]]:
        if not hasattr(self, "_buckets"):
            field = getattr(self._meta["aggs"], "field", None)
            if field:
                self._meta["field"] = self._meta["search"]._resolve_field(field)
            bs = cast(Union[Dict[str, Any], List[Any]], self._d_["buckets"])
            if isinstance(bs, list):
                ret = AttrList(bs, obj_wrapper=self._wrap_bucket)
            else:
                ret = AttrDict[Any]({k: self._wrap_bucket(bs[k]) for k in bs})  # type: ignore[assignment]
            super(AttrDict, self).__setattr__("_buckets", ret)
        return self._buckets


class FieldBucketData(BucketData[_R]):
    _bucket_class = FieldBucket


class TopHitsData(Response[_R]):
    def __init__(self, agg: "Agg[_R]", search: "SearchBase[_R]", data: Any):
        super(AttrDict, self).__setattr__(
            "meta", AttrDict({"agg": agg, "search": search})
        )
        super().__init__(search, data)
