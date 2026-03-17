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

from typing import List, Optional, Union

from typing_extensions import Self

from ..async_connections import get_connection
from ..mapping_base import MappingBase
from ..utils import AsyncUsingType


class AsyncMapping(MappingBase):
    @classmethod
    async def from_es(
        cls, index: Optional[Union[str, List[str]]], using: AsyncUsingType = "default"
    ) -> Self:
        m = cls()
        await m.update_from_es(index, using)
        return m

    async def update_from_es(
        self, index: Optional[Union[str, List[str]]], using: AsyncUsingType = "default"
    ) -> None:
        es = get_connection(using)
        raw = await es.indices.get_mapping(index=index)
        _, raw = raw.popitem()
        self._update_from_dict(raw["mappings"])

    async def save(self, index: str, using: AsyncUsingType = "default") -> None:
        from .index import AsyncIndex

        i = AsyncIndex(index, using=using)
        i.mapping(self)
        await i.save()
