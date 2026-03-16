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

from typing import TYPE_CHECKING, Any, Dict, Optional

from typing_extensions import Self

from ..async_connections import get_connection
from ..exceptions import IllegalOperation
from ..index_base import IndexBase
from ..utils import AsyncUsingType
from .mapping import AsyncMapping
from .search import AsyncSearch
from .update_by_query import AsyncUpdateByQuery

if TYPE_CHECKING:
    from elastic_transport import ObjectApiResponse

    from elasticsearch import AsyncElasticsearch


class AsyncIndexTemplate:
    def __init__(
        self,
        name: str,
        template: str,
        index: Optional["AsyncIndex"] = None,
        order: Optional[int] = None,
        **kwargs: Any,
    ):
        if index is None:
            self._index = AsyncIndex(template, **kwargs)
        else:
            if kwargs:
                raise ValueError(
                    "You cannot specify options for Index when"
                    " passing an Index instance."
                )
            self._index = index.clone()
            self._index._name = template
        self._template_name = name
        self.order = order

    def __getattr__(self, attr_name: str) -> Any:
        return getattr(self._index, attr_name)

    def to_dict(self) -> Dict[str, Any]:
        d = self._index.to_dict()
        d["index_patterns"] = [self._index._name]
        if self.order is not None:
            d["order"] = self.order
        return d

    async def save(
        self, using: Optional[AsyncUsingType] = None
    ) -> "ObjectApiResponse[Any]":
        es = get_connection(using or self._index._using)
        return await es.indices.put_template(
            name=self._template_name, body=self.to_dict()
        )


class AsyncComposableIndexTemplate:
    def __init__(
        self,
        name: str,
        template: str,
        index: Optional["AsyncIndex"] = None,
        priority: Optional[int] = None,
        **kwargs: Any,
    ):
        if index is None:
            self._index = AsyncIndex(template, **kwargs)
        else:
            if kwargs:
                raise ValueError(
                    "You cannot specify options for Index when"
                    " passing an Index instance."
                )
            self._index = index.clone()
            self._index._name = template
        self._template_name = name
        self.priority = priority

    def __getattr__(self, attr_name: str) -> Any:
        return getattr(self._index, attr_name)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"template": self._index.to_dict()}
        d["index_patterns"] = [self._index._name]
        if self.priority is not None:
            d["priority"] = self.priority
        return d

    async def save(
        self, using: Optional[AsyncUsingType] = None
    ) -> "ObjectApiResponse[Any]":
        es = get_connection(using or self._index._using)
        return await es.indices.put_index_template(
            name=self._template_name, **self.to_dict()
        )


class AsyncIndex(IndexBase):
    _using: AsyncUsingType

    if TYPE_CHECKING:

        def get_or_create_mapping(self) -> AsyncMapping: ...

    def __init__(self, name: str, using: AsyncUsingType = "default"):
        """
        :arg name: name of the index
        :arg using: connection alias to use, defaults to ``'default'``
        """
        super().__init__(name, AsyncMapping, using=using)

    def _get_connection(
        self, using: Optional[AsyncUsingType] = None
    ) -> "AsyncElasticsearch":
        if self._name is None:
            raise ValueError("You cannot perform API calls on the default index.")
        return get_connection(using or self._using)

    connection = property(_get_connection)

    def as_template(
        self,
        template_name: str,
        pattern: Optional[str] = None,
        order: Optional[int] = None,
    ) -> AsyncIndexTemplate:
        return AsyncIndexTemplate(
            template_name, pattern or self._name, index=self, order=order
        )

    def as_composable_template(
        self,
        template_name: str,
        pattern: Optional[str] = None,
        priority: Optional[int] = None,
    ) -> AsyncComposableIndexTemplate:
        return AsyncComposableIndexTemplate(
            template_name, pattern or self._name, index=self, priority=priority
        )

    async def load_mappings(self, using: Optional[AsyncUsingType] = None) -> None:
        await self.get_or_create_mapping().update_from_es(
            self._name, using=using or self._using
        )

    def clone(
        self, name: Optional[str] = None, using: Optional[AsyncUsingType] = None
    ) -> Self:
        """
        Create a copy of the instance with another name or connection alias.
        Useful for creating multiple indices with shared configuration::

            i = Index('base-index')
            i.settings(number_of_shards=1)
            i.create()

            i2 = i.clone('other-index')
            i2.create()

        :arg name: name of the index
        :arg using: connection alias to use, defaults to ``'default'``
        """
        i = self.__class__(name or self._name, using=using or self._using)
        i._settings = self._settings.copy()
        i._aliases = self._aliases.copy()
        i._analysis = self._analysis.copy()
        i._doc_types = self._doc_types[:]
        if self._mapping is not None:
            i._mapping = self._mapping._clone()
        return i

    def search(self, using: Optional[AsyncUsingType] = None) -> AsyncSearch:
        """
        Return a :class:`~elasticsearch.dsl.Search` object searching over the
        index (or all the indices belonging to this template) and its
        ``Document``\\s.
        """
        return AsyncSearch(
            using=using or self._using, index=self._name, doc_type=self._doc_types
        )

    def updateByQuery(
        self, using: Optional[AsyncUsingType] = None
    ) -> AsyncUpdateByQuery:
        """
        Return a :class:`~elasticsearch.dsl.UpdateByQuery` object searching over the index
        (or all the indices belonging to this template) and updating Documents that match
        the search criteria.

        For more information, see here:
        https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
        """
        return AsyncUpdateByQuery(
            using=using or self._using,
            index=self._name,
        )

    async def create(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Creates the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.create`` unchanged.
        """
        return await self._get_connection(using).indices.create(
            index=self._name, body=self.to_dict(), **kwargs
        )

    async def is_closed(self, using: Optional[AsyncUsingType] = None) -> bool:
        state = await self._get_connection(using).cluster.state(
            index=self._name, metric="metadata"
        )
        return bool(state["metadata"]["indices"][self._name]["state"] == "close")

    async def save(
        self, using: Optional[AsyncUsingType] = None
    ) -> "Optional[ObjectApiResponse[Any]]":
        """
        Sync the index definition with elasticsearch, creating the index if it
        doesn't exist and updating its settings and mappings if it does.

        Note some settings and mapping changes cannot be done on an open
        index (or at all on an existing index) and for those this method will
        fail with the underlying exception.
        """
        if not await self.exists(using=using):
            return await self.create(using=using)

        body = self.to_dict()
        settings = body.pop("settings", {})
        analysis = settings.pop("analysis", None)
        current_settings = (await self.get_settings(using=using))[self._name][
            "settings"
        ]["index"]
        if analysis:
            if await self.is_closed(using=using):
                # closed index, update away
                settings["analysis"] = analysis
            else:
                # compare analysis definition, if all analysis objects are
                # already defined as requested, skip analysis update and
                # proceed, otherwise raise IllegalOperation
                existing_analysis = current_settings.get("analysis", {})
                if any(
                    existing_analysis.get(section, {}).get(k, None)
                    != analysis[section][k]
                    for section in analysis
                    for k in analysis[section]
                ):
                    raise IllegalOperation(
                        "You cannot update analysis configuration on an open index, "
                        "you need to close index %s first." % self._name
                    )

        # try and update the settings
        if settings:
            settings = settings.copy()
            for k, v in list(settings.items()):
                if k in current_settings and current_settings[k] == str(v):
                    del settings[k]

            if settings:
                await self.put_settings(using=using, body=settings)

        # update the mappings, any conflict in the mappings will result in an
        # exception
        mappings = body.pop("mappings", {})
        if mappings:
            return await self.put_mapping(using=using, body=mappings)

        return None

    async def analyze(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Perform the analysis process on a text and return the tokens breakdown
        of the text.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.analyze`` unchanged.
        """
        return await self._get_connection(using).indices.analyze(
            index=self._name, **kwargs
        )

    async def refresh(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Performs a refresh operation on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.refresh`` unchanged.
        """
        return await self._get_connection(using).indices.refresh(
            index=self._name, **kwargs
        )

    async def flush(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Performs a flush operation on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.flush`` unchanged.
        """
        return await self._get_connection(using).indices.flush(
            index=self._name, **kwargs
        )

    async def get(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        The get index API allows to retrieve information about the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get`` unchanged.
        """
        return await self._get_connection(using).indices.get(index=self._name, **kwargs)

    async def open(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Opens the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.open`` unchanged.
        """
        return await self._get_connection(using).indices.open(
            index=self._name, **kwargs
        )

    async def close(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Closes the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.close`` unchanged.
        """
        return await self._get_connection(using).indices.close(
            index=self._name, **kwargs
        )

    async def delete(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Deletes the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.delete`` unchanged.
        """
        return await self._get_connection(using).indices.delete(
            index=self._name, **kwargs
        )

    async def exists(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> bool:
        """
        Returns ``True`` if the index already exists in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.exists`` unchanged.
        """
        return bool(
            await self._get_connection(using).indices.exists(index=self._name, **kwargs)
        )

    async def put_mapping(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Register specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_mapping`` unchanged.
        """
        return await self._get_connection(using).indices.put_mapping(
            index=self._name, **kwargs
        )

    async def get_mapping(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Retrieve specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_mapping`` unchanged.
        """
        return await self._get_connection(using).indices.get_mapping(
            index=self._name, **kwargs
        )

    async def get_field_mapping(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Retrieve mapping definition of a specific field.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_field_mapping`` unchanged.
        """
        return await self._get_connection(using).indices.get_field_mapping(
            index=self._name, **kwargs
        )

    async def put_alias(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Create an alias for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_alias`` unchanged.
        """
        return await self._get_connection(using).indices.put_alias(
            index=self._name, **kwargs
        )

    async def exists_alias(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> bool:
        """
        Return a boolean indicating whether given alias exists for this index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.exists_alias`` unchanged.
        """
        return bool(
            await self._get_connection(using).indices.exists_alias(
                index=self._name, **kwargs
            )
        )

    async def get_alias(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Retrieve a specified alias.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_alias`` unchanged.
        """
        return await self._get_connection(using).indices.get_alias(
            index=self._name, **kwargs
        )

    async def delete_alias(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Delete specific alias.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.delete_alias`` unchanged.
        """
        return await self._get_connection(using).indices.delete_alias(
            index=self._name, **kwargs
        )

    async def get_settings(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Retrieve settings for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_settings`` unchanged.
        """
        return await self._get_connection(using).indices.get_settings(
            index=self._name, **kwargs
        )

    async def put_settings(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Change specific index level settings in real time.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_settings`` unchanged.
        """
        return await self._get_connection(using).indices.put_settings(
            index=self._name, **kwargs
        )

    async def stats(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Retrieve statistics on different operations happening on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.stats`` unchanged.
        """
        return await self._get_connection(using).indices.stats(
            index=self._name, **kwargs
        )

    async def segments(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Provide low level segments information that a Lucene index (shard
        level) is built with.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.segments`` unchanged.
        """
        return await self._get_connection(using).indices.segments(
            index=self._name, **kwargs
        )

    async def validate_query(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Validate a potentially expensive query without executing it.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.validate_query`` unchanged.
        """
        return await self._get_connection(using).indices.validate_query(
            index=self._name, **kwargs
        )

    async def clear_cache(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Clear all caches or specific cached associated with the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.clear_cache`` unchanged.
        """
        return await self._get_connection(using).indices.clear_cache(
            index=self._name, **kwargs
        )

    async def recovery(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        The indices recovery API provides insight into on-going shard
        recoveries for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.recovery`` unchanged.
        """
        return await self._get_connection(using).indices.recovery(
            index=self._name, **kwargs
        )

    async def shard_stores(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        Provides store information for shard copies of the index. Store
        information reports on which nodes shard copies exist, the shard copy
        version, indicating how recent they are, and any exceptions encountered
        while opening the shard index or from earlier engine failure.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.shard_stores`` unchanged.
        """
        return await self._get_connection(using).indices.shard_stores(
            index=self._name, **kwargs
        )

    async def forcemerge(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        The force merge API allows to force merging of the index through an
        API. The merge relates to the number of segments a Lucene index holds
        within each shard. The force merge operation allows to reduce the
        number of segments by merging them.

        This call will block until the merge is complete. If the http
        connection is lost, the request will continue in the background, and
        any new requests will block until the previous force merge is complete.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.forcemerge`` unchanged.
        """
        return await self._get_connection(using).indices.forcemerge(
            index=self._name, **kwargs
        )

    async def shrink(
        self, using: Optional[AsyncUsingType] = None, **kwargs: Any
    ) -> "ObjectApiResponse[Any]":
        """
        The shrink index API allows you to shrink an existing index into a new
        index with fewer primary shards. The number of primary shards in the
        target index must be a factor of the shards in the source index. For
        example an index with 8 primary shards can be shrunk into 4, 2 or 1
        primary shards or an index with 15 primary shards can be shrunk into 5,
        3 or 1. If the number of shards in the index is a prime number it can
        only be shrunk into a single primary shard. Before shrinking, a
        (primary or replica) copy of every shard in the index must be present
        on the same node.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.shrink`` unchanged.
        """
        return await self._get_connection(using).indices.shrink(
            index=self._name, **kwargs
        )
