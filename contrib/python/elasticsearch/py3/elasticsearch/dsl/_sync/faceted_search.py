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

from ..faceted_search_base import FacetedResponse, FacetedSearchBase
from ..utils import _R
from .search import Search

if TYPE_CHECKING:
    from ..response import Response


class FacetedSearch(FacetedSearchBase[_R]):
    _s: Search[_R]

    def count(self) -> int:
        return self._s.count()

    def search(self) -> Search[_R]:
        """
        Returns the base Search object to which the facets are added.

        You can customize the query by overriding this method and returning a
        modified search object.
        """
        s = Search[_R](doc_type=self.doc_types, index=self.index, using=self.using)
        return s.response_class(FacetedResponse)

    def execute(self) -> "Response[_R]":
        """
        Execute the search and return the response.
        """
        r = self._s.execute()
        r._faceted_search = self
        return r
