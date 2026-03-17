# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.


from typing import Any

from opensearchpy._async.helpers.search import AsyncSearch
from opensearchpy.helpers.faceted_search import FacetedResponse
from opensearchpy.helpers.query import MatchAll


class AsyncFacetedSearch:
    """
    Abstraction for creating faceted navigation searches that takes care of
    composing the queries, aggregations and filters as needed as well as
    presenting the results in an easy-to-consume fashion::

        class BlogSearch(AsyncFacetedSearch):
            index = 'blogs'
            doc_types = [Blog, Post]
            fields = ['title^5', 'category', 'description', 'body']

            facets = {
                'type': TermsFacet(field='_type'),
                'category': TermsFacet(field='category'),
                'weekly_posts': DateHistogramFacet(field='published_from', interval='week')
            }

            def search(self):
                ' Override search to add your own filters '
                s = super(BlogSearch, self).search()
                return s.filter('term', published=True)

        # when using:
        blog_search = BlogSearch("web framework", filters={"category": "python"})

        # supports pagination
        blog_search[10:20]

        response = await blog_search.execute()

        # easy access to aggregation results:
        for category, hit_count, is_selected in response.facets.category:
            print(
                "Category %s has %d hits%s." % (
                    category,
                    hit_count,
                    ' and is chosen' if is_selected else ''
                )
            )

    """

    index: Any = None
    doc_types: Any = None
    fields: Any = None
    facets: Any = {}
    using: str = "default"

    def __init__(self, query: Any = None, filters: Any = {}, sort: Any = ()) -> None:
        """
        :arg query: the text to search for
        :arg filters: facet values to filter
        :arg sort: sort information to be passed to :class:`~opensearchpy.AsyncSearch`
        """
        self._query = query
        self._filters: Any = {}
        self._sort = sort
        self.filter_values: Any = {}
        for name, value in filters.items():
            self.add_filter(name, value)

        self._s = self.build_search()

    async def count(self) -> Any:
        return await self._s.count()

    def __getitem__(self, k: Any) -> Any:
        self._s = self._s[k]
        return self

    def __iter__(self) -> Any:
        return iter(self._s)

    def add_filter(self, name: Any, filter_values: Any) -> None:
        """
        Add a filter for a facet.
        """
        # normalize the value into a list
        if not isinstance(filter_values, (tuple, list)):
            if filter_values is None:
                return
            filter_values = [
                filter_values,
            ]

        # remember the filter values for use in FacetedResponse
        self.filter_values[name] = filter_values

        # get the filter from the facet
        f = self.facets[name].add_filter(filter_values)
        if f is None:
            return

        self._filters[name] = f

    def search(self) -> Any:
        """
        Returns the base Search object to which the facets are added.

        You can customize the query by overriding this method and returning a
        modified search object.
        """
        s = AsyncSearch(doc_type=self.doc_types, index=self.index, using=self.using)
        return s.response_class(FacetedResponse)

    def query(self, search: Any, query: Any) -> Any:
        """
        Add query part to ``search``.

        Override this if you wish to customize the query used.
        """
        if query:
            if self.fields:
                return search.query("multi_match", fields=self.fields, query=query)
            else:
                return search.query("multi_match", query=query)
        return search

    def aggregate(self, search: Any) -> Any:
        """
        Add aggregations representing the facets selected, including potential
        filters.
        """
        for f, facet in self.facets.items():
            agg = facet.get_aggregation()
            agg_filter = MatchAll()
            for field, filter in self._filters.items():
                if f == field:
                    continue
                agg_filter &= filter
            search.aggs.bucket("_filter_" + f, "filter", filter=agg_filter).bucket(
                f, agg
            )

    def filter(self, search: Any) -> Any:
        """
        Add a ``post_filter`` to the search request narrowing the results based
        on the facet filters.
        """
        if not self._filters:
            return search

        post_filter = MatchAll()
        for f in self._filters.values():
            post_filter &= f
        return search.post_filter(post_filter)

    def highlight(self, search: Any) -> Any:
        """
        Add highlighting for all the fields
        """
        return search.highlight(
            *(f if "^" not in f else f.split("^", 1)[0] for f in self.fields)
        )

    def sort(self, search: Any) -> Any:
        """
        Add sorting information to the request.
        """
        if self._sort:
            search = search.sort(*self._sort)
        return search

    def build_search(self) -> Any:
        """
        Construct the ``AsyncSearch`` object.
        """
        s = self.search()
        s = self.query(s, self._query)
        s = self.filter(s)
        if self.fields:
            s = self.highlight(s)
        s = self.sort(s)
        self.aggregate(s)
        return s

    async def execute(self) -> Any:
        """
        Execute the search and return the response.
        """
        r = await self._s.execute()
        r._faceted_search = self
        return r
