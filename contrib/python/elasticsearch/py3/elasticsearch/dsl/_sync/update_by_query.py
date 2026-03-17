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

from typing import TYPE_CHECKING

from ..connections import get_connection
from ..update_by_query_base import UpdateByQueryBase
from ..utils import _R, UsingType

if TYPE_CHECKING:
    from ..response import UpdateByQueryResponse


class UpdateByQuery(UpdateByQueryBase[_R]):
    _using: UsingType

    def execute(self) -> "UpdateByQueryResponse[_R]":
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.
        """
        es = get_connection(self._using)
        assert self._index is not None

        self._response = self._response_class(
            self,
            (
                es.update_by_query(index=self._index, **self.to_dict(), **self._params)
            ).body,
        )
        return self._response
