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

from typing import Any, Dict, List, Tuple, cast

from ..utils import AttrDict, HitMeta


class Hit(AttrDict[Any]):
    def __init__(self, document: Dict[str, Any]):
        data: Dict[str, Any] = {}
        if "_source" in document:
            data = cast(Dict[str, Any], document["_source"])
        if "fields" in document:
            data.update(cast(Dict[str, Any], document["fields"]))

        super().__init__(data)
        # assign meta as attribute and not as key in self._d_
        super(AttrDict, self).__setattr__("meta", HitMeta(document))

    def __getstate__(self) -> Tuple[Dict[str, Any], HitMeta]:  # type: ignore[override]
        # add self.meta since it is not in self.__dict__
        return super().__getstate__() + (self.meta,)

    def __setstate__(self, state: Tuple[Dict[str, Any], HitMeta]) -> None:  # type: ignore[override]
        super(AttrDict, self).__setattr__("meta", state[-1])
        super().__setstate__(state[:-1])

    def __dir__(self) -> List[str]:
        # be sure to expose meta in dir(self)
        return super().__dir__() + ["meta"]

    def __repr__(self) -> str:
        return "<Hit({}): {}>".format(
            "/".join(
                getattr(self.meta, key) for key in ("index", "id") if key in self.meta
            ),
            super().__repr__(),
        )
