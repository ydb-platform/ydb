# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from typing import Any

from opensearchpy._async.helpers.mapping import AsyncMapping
from opensearchpy._async.helpers.search import AsyncSearch
from opensearchpy._async.helpers.update_by_query import AsyncUpdateByQuery
from opensearchpy.connection.async_connections import get_connection
from opensearchpy.exceptions import IllegalOperation, ValidationException
from opensearchpy.helpers import analysis
from opensearchpy.helpers.utils import merge


class AsyncIndexTemplate:
    def __init__(
        self,
        name: Any,
        template: Any,
        index: Any = None,
        order: Any = None,
        **kwargs: Any
    ) -> None:
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

    def __getattr__(self, attr_name: Any) -> Any:
        return getattr(self._index, attr_name)

    def to_dict(self) -> Any:
        d = self._index.to_dict()
        d["index_patterns"] = [self._index._name]
        if self.order is not None:
            d["order"] = self.order
        return d

    async def save(self, using: Any = None) -> Any:
        opensearch = await get_connection(using or self._index._using)
        return await opensearch.indices.put_template(
            name=self._template_name, body=self.to_dict()
        )


class AsyncIndex:
    def __init__(self, name: Any, using: Any = "default") -> None:
        """
        :arg name: name of the index
        :arg using: connection alias to use, defaults to ``'default'``
        """
        self._name = name
        self._doc_types: Any = []
        self._using = using
        self._settings: Any = {}
        self._aliases: Any = {}
        self._analysis: Any = {}
        self._mapping: Any = None

    def get_or_create_mapping(self) -> Any:
        if self._mapping is None:
            self._mapping = AsyncMapping()
        return self._mapping

    def as_template(
        self, template_name: Any, pattern: Any = None, order: Any = None
    ) -> Any:
        # TODO: should we allow pattern to be a top-level arg?
        # or maybe have an IndexPattern that allows for it and have
        # AsyncDocument._index be that?
        return AsyncIndexTemplate(
            template_name, pattern or self._name, index=self, order=order
        )

    def resolve_nested(self, field_path: Any) -> Any:
        for doc in self._doc_types:
            nested, field = doc._doc_type.mapping.resolve_nested(field_path)
            if field is not None:
                return nested, field
        if self._mapping:
            return self._mapping.resolve_nested(field_path)
        return (), None

    def resolve_field(self, field_path: Any) -> Any:
        for doc in self._doc_types:
            field = doc._doc_type.mapping.resolve_field(field_path)
            if field is not None:
                return field
        if self._mapping:
            return self._mapping.resolve_field(field_path)
        return None

    async def load_mappings(self, using: Any = None) -> None:
        await self.get_or_create_mapping().update_from_opensearch(
            self._name, using=using or self._using
        )

    def clone(self, name: Any = None, using: Any = None) -> Any:
        """
        Create a copy of the instance with another name or connection alias.
        Useful for creating multiple indices with shared configuration::

            i = AsyncIndex('base-index')
            i.settings(number_of_shards=1)
            await i.create()

            i2 = i.clone('other-index')
            await i2.create()

        :arg name: name of the index
        :arg using: connection alias to use, defaults to ``'default'``
        """
        i = AsyncIndex(name or self._name, using=using or self._using)
        i._settings = self._settings.copy()
        i._aliases = self._aliases.copy()
        i._analysis = self._analysis.copy()
        i._doc_types = self._doc_types[:]
        if self._mapping is not None:
            i._mapping = self._mapping._clone()
        return i

    async def _get_connection(self, using: Any = None) -> Any:
        if self._name is None:
            raise ValueError("You cannot perform API calls on the default index.")
        return await get_connection(using or self._using)

    connection = property(_get_connection)

    def mapping(self, mapping: Any) -> None:
        """
        Associate a mapping (an instance of
        :class:`~opensearchpy.AsyncMapping`) with this index.
        This means that, when this index is created, it will contain the
        mappings for the document type defined by those mappings.
        """
        self.get_or_create_mapping().update(mapping)

    def document(self, document: Any) -> Any:
        """
        Associate a :class:`~opensearchpy.AsyncDocument` subclass with an index.
        This means that, when this index is created, it will contain the
        mappings for the ``AsyncDocument``. If the ``AsyncDocument`` class doesn't have a
        default index yet (by defining ``class AsyncIndex``), this instance will be
        used. Can be used as a decorator::

            i = AsyncIndex('blog')

            @i.document
            class Post(AsyncDocument):
                title = Text()

            # create the index, including Post mappings
            await i.create()

            # .search() will now return a AsyncSearch object that will return
            # properly deserialized Post instances
            s = i.search()
        """
        self._doc_types.append(document)

        # If the document index does not have any name, that means the user
        # did not set any index already to the document.
        # So set this index as document index
        if document._index._name is None:
            document._index = self

        return document

    def settings(self, **kwargs: Any) -> "AsyncIndex":
        """
        Add settings to the index::

            i = AsyncIndex('i')
            i.settings(number_of_shards=1, number_of_replicas=0)

        Multiple calls to ``settings`` will merge the keys, later overriding
        the earlier.
        """
        self._settings.update(kwargs)
        return self

    def aliases(self, **kwargs: Any) -> "AsyncIndex":
        """
        Add aliases to the index definition::

            i = AsyncIndex('blog-v2')
            i.aliases(blog={}, published={'filter': Q('term', published=True)})
        """
        self._aliases.update(kwargs)
        return self

    def analyzer(self, *args: Any, **kwargs: Any) -> Any:
        """
        Explicitly add an analyzer to an index. Note that all custom analyzers
        defined in mappings will also be created. This is useful for search analyzers.

        Example::

            from opensearchpy import analyzer, tokenizer

            my_analyzer = analyzer('my_analyzer',
                tokenizer=tokenizer('trigram', 'nGram', min_gram=3, max_gram=3),
                filter=['lowercase']
            )

            i = AsyncIndex('blog')
            i.analyzer(my_analyzer)

        """
        analyzer = analysis.analyzer(*args, **kwargs)
        d = analyzer.get_analysis_definition()
        # empty custom analyzer, probably already defined out of our control
        if not d:
            return

        # merge the definition
        merge(self._analysis, d, True)

    def to_dict(self) -> Any:
        out = {}
        if self._settings:
            out["settings"] = self._settings
        if self._aliases:
            out["aliases"] = self._aliases
        mappings: Any = self._mapping.to_dict() if self._mapping else {}
        analysis: Any = self._mapping._collect_analysis() if self._mapping else {}
        for d in self._doc_types:
            mapping = d._doc_type.mapping
            merge(mappings, mapping.to_dict(), True)
            merge(analysis, mapping._collect_analysis(), True)
        if mappings:
            out["mappings"] = mappings
        if analysis or self._analysis:
            merge(analysis, self._analysis)
            out.setdefault("settings", {})["analysis"] = analysis
        return out

    def search(self, using: Any = None) -> Any:
        """
        Return a :class:`~opensearchpy.AsyncSearch` object searching over the
        index (or all the indices belonging to this template) and its
        ``Document``\\s.
        """
        return AsyncSearch(
            using=using or self._using, index=self._name, doc_type=self._doc_types
        )

    def updateByQuery(self, using: Any = None) -> Any:  # pylint: disable=invalid-name
        """
        Return a :class:`~opensearchpy.AsyncUpdateByQuery` object searching over the index
        (or all the indices belonging to this template) and updating Documents that match
        the search criteria.

        For more information, see here:
        https://opensearch.org/docs/latest/opensearch/rest-api/document-apis/update-by-query/
        """
        return AsyncUpdateByQuery(
            using=using or self._using,
            index=self._name,
        )

    async def create(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Creates the index in opensearch.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.create`` unchanged.
        """
        return await (await self._get_connection(using)).indices.create(
            index=self._name, body=self.to_dict(), **kwargs
        )

    async def is_closed(self, using: Any = None) -> Any:
        state = await (await self._get_connection(using)).cluster.state(
            index=self._name, metric="metadata"
        )
        return state["metadata"]["indices"][self._name]["state"] == "close"

    async def save(self, using: Any = None) -> Any:
        """
        Sync the index definition with opensearch, creating the index if it
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

        # If _name points to an alias, the response object will contain keys with
        # the index name(s) the alias points to. If the alias points to multiple
        # indices, raise exception as the intention is ambiguous
        settings_response = await self.get_settings(using=using)
        if len(settings_response) > 1:
            raise ValidationException(
                "Settings for %s point to multiple indices: %s."
                % (self._name, ", ".join(list(settings_response.keys())))
            )
        current_settings = settings_response.popitem()[1]["settings"]["index"]

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
            await self.put_mapping(using=using, body=mappings)

    async def analyze(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Perform the analysis process on a text and return the tokens breakdown
        of the text.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.analyze`` unchanged.
        """
        return await (await self._get_connection(using)).indices.analyze(
            index=self._name, **kwargs
        )

    async def refresh(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Performs a refresh operation on the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.refresh`` unchanged.
        """
        return await (await self._get_connection(using)).indices.refresh(
            index=self._name, **kwargs
        )

    async def flush(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Performs a flush operation on the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.flush`` unchanged.
        """
        return await (await self._get_connection(using)).indices.flush(
            index=self._name, **kwargs
        )

    async def get(self, using: Any = None, **kwargs: Any) -> Any:
        """
        The get index API allows to retrieve information about the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.get`` unchanged.
        """
        return await (await self._get_connection(using)).indices.get(
            index=self._name, **kwargs
        )

    async def open(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Opens the index in opensearch.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.open`` unchanged.
        """
        return await (await self._get_connection(using)).indices.open(
            index=self._name, **kwargs
        )

    async def close(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Closes the index in opensearch.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.close`` unchanged.
        """
        return await (await self._get_connection(using)).indices.close(
            index=self._name, **kwargs
        )

    async def delete(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Deletes the index in opensearch.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.delete`` unchanged.
        """
        return await (await self._get_connection(using)).indices.delete(
            index=self._name, **kwargs
        )

    async def exists(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Returns ``True`` if the index already exists in opensearch.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.exists`` unchanged.
        """
        return await (await self._get_connection(using)).indices.exists(
            index=self._name, **kwargs
        )

    async def put_mapping(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Register specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.put_mapping`` unchanged.
        """
        return await (await self._get_connection(using)).indices.put_mapping(
            index=self._name, **kwargs
        )

    async def get_mapping(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Retrieve specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.get_mapping`` unchanged.
        """
        return await (await self._get_connection(using)).indices.get_mapping(
            index=self._name, **kwargs
        )

    async def get_field_mapping(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Retrieve mapping definition of a specific field.

        Any additional keyword arguments will be passed to
        ``Async OpenSearch.indices.get_field_mapping`` unchanged.
        """
        return await (await self._get_connection(using)).indices.get_field_mapping(
            index=self._name, **kwargs
        )

    async def put_alias(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Create an alias for the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.put_alias`` unchanged.
        """
        return await (await self._get_connection(using)).indices.put_alias(
            index=self._name, **kwargs
        )

    async def exists_alias(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Return a boolean indicating whether given alias exists for this index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.exists_alias`` unchanged.
        """
        return await (await self._get_connection(using)).indices.exists_alias(
            index=self._name, **kwargs
        )

    async def get_alias(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Retrieve a specified alias.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.get_alias`` unchanged.
        """
        return await (await self._get_connection(using)).indices.get_alias(
            index=self._name, **kwargs
        )

    async def delete_alias(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Delete specific alias.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.delete_alias`` unchanged.
        """
        return await (await self._get_connection(using)).indices.delete_alias(
            index=self._name, **kwargs
        )

    async def get_settings(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Retrieve settings for the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.get_settings`` unchanged.
        """
        return await (await self._get_connection(using)).indices.get_settings(
            index=self._name, **kwargs
        )

    async def put_settings(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Change specific index level settings in real time.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.put_settings`` unchanged.
        """
        return await (await self._get_connection(using)).indices.put_settings(
            index=self._name, **kwargs
        )

    async def stats(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Retrieve statistics on different operations happening on the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.stats`` unchanged.
        """
        return await (await self._get_connection(using)).indices.stats(
            index=self._name, **kwargs
        )

    async def segments(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Provide low level segments information that a Lucene index (shard
        level) is built with.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.segments`` unchanged.
        """
        return await (await self._get_connection(using)).indices.segments(
            index=self._name, **kwargs
        )

    async def validate_query(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Validate a potentially expensive query without executing it.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.validate_query`` unchanged.
        """
        return await (await self._get_connection(using)).indices.validate_query(
            index=self._name, **kwargs
        )

    async def clear_cache(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Clear all caches or specific cached associated with the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.clear_cache`` unchanged.
        """
        return await (await self._get_connection(using)).indices.clear_cache(
            index=self._name, **kwargs
        )

    async def recovery(self, using: Any = None, **kwargs: Any) -> Any:
        """
        The indices recovery API provides insight into on-going shard
        recoveries for the index.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.recovery`` unchanged.
        """
        return await (await self._get_connection(using)).indices.recovery(
            index=self._name, **kwargs
        )

    async def upgrade(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Upgrade the index to the latest format.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.upgrade`` unchanged.
        """
        return await (await self._get_connection(using)).indices.upgrade(
            index=self._name, **kwargs
        )

    async def get_upgrade(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Monitor how much of the index is upgraded.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.get_upgrade`` unchanged.
        """
        return await (await self._get_connection(using)).indices.get_upgrade(
            index=self._name, **kwargs
        )

    async def shard_stores(self, using: Any = None, **kwargs: Any) -> Any:
        """
        Provides store information for shard copies of the index. Store
        information reports on which nodes shard copies exist, the shard copy
        version, indicating how recent they are, and any exceptions encountered
        while opening the shard index or from earlier engine failure.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.shard_stores`` unchanged.
        """
        return await (await self._get_connection(using)).indices.shard_stores(
            index=self._name, **kwargs
        )

    async def forcemerge(self, using: Any = None, **kwargs: Any) -> Any:
        """
        The force merge API allows to force merging of the index through an
        API. The merge relates to the number of segments a Lucene index holds
        within each shard. The force merge operation allows to reduce the
        number of segments by merging them.

        This call will block until the merge is complete. If the http
        connection is lost, the request will continue in the background, and
        any new requests will block until the previous force merge is complete.

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.indices.forcemerge`` unchanged.
        """
        return await (await self._get_connection(using)).indices.forcemerge(
            index=self._name, **kwargs
        )

    async def shrink(self, using: Any = None, **kwargs: Any) -> Any:
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
        ``AsyncOpenSearch.indices.shrink`` unchanged.
        """
        return await (await self._get_connection(using)).indices.shrink(
            index=self._name, **kwargs
        )
