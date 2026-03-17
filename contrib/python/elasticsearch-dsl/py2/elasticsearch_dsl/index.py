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

from . import analysis
from .connections import CLIENT_HAS_NAMED_BODY_PARAMS, get_connection
from .exceptions import IllegalOperation
from .mapping import Mapping
from .search import Search
from .update_by_query import UpdateByQuery
from .utils import merge


class IndexTemplate(object):
    def __init__(self, name, template, index=None, order=None, **kwargs):
        if index is None:
            self._index = Index(template, **kwargs)
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

    def __getattr__(self, attr_name):
        return getattr(self._index, attr_name)

    def to_dict(self):
        d = self._index.to_dict()
        d["index_patterns"] = [self._index._name]
        if self.order is not None:
            d["order"] = self.order
        return d

    def save(self, using=None):

        es = get_connection(using or self._index._using)
        return es.indices.put_template(name=self._template_name, body=self.to_dict())


class Index(object):
    def __init__(self, name, using="default"):
        """
        :arg name: name of the index
        :arg using: connection alias to use, defaults to ``'default'``
        """
        self._name = name
        self._doc_types = []
        self._using = using
        self._settings = {}
        self._aliases = {}
        self._analysis = {}
        self._mapping = None

    def get_or_create_mapping(self):
        if self._mapping is None:
            self._mapping = Mapping()
        return self._mapping

    def as_template(self, template_name, pattern=None, order=None):
        # TODO: should we allow pattern to be a top-level arg?
        # or maybe have an IndexPattern that allows for it and have
        # Document._index be that?
        return IndexTemplate(
            template_name, pattern or self._name, index=self, order=order
        )

    def resolve_nested(self, field_path):
        for doc in self._doc_types:
            nested, field = doc._doc_type.mapping.resolve_nested(field_path)
            if field is not None:
                return nested, field
        if self._mapping:
            return self._mapping.resolve_nested(field_path)
        return (), None

    def resolve_field(self, field_path):
        for doc in self._doc_types:
            field = doc._doc_type.mapping.resolve_field(field_path)
            if field is not None:
                return field
        if self._mapping:
            return self._mapping.resolve_field(field_path)
        return None

    def load_mappings(self, using=None):
        self.get_or_create_mapping().update_from_es(
            self._name, using=using or self._using
        )

    def clone(self, name=None, using=None):
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
        i = Index(name or self._name, using=using or self._using)
        i._settings = self._settings.copy()
        i._aliases = self._aliases.copy()
        i._analysis = self._analysis.copy()
        i._doc_types = self._doc_types[:]
        if self._mapping is not None:
            i._mapping = self._mapping._clone()
        return i

    def _get_connection(self, using=None):
        if self._name is None:
            raise ValueError("You cannot perform API calls on the default index.")
        return get_connection(using or self._using)

    connection = property(_get_connection)

    def mapping(self, mapping):
        """
        Associate a mapping (an instance of
        :class:`~elasticsearch_dsl.Mapping`) with this index.
        This means that, when this index is created, it will contain the
        mappings for the document type defined by those mappings.
        """
        self.get_or_create_mapping().update(mapping)

    def document(self, document):
        """
        Associate a :class:`~elasticsearch_dsl.Document` subclass with an index.
        This means that, when this index is created, it will contain the
        mappings for the ``Document``. If the ``Document`` class doesn't have a
        default index yet (by defining ``class Index``), this instance will be
        used. Can be used as a decorator::

            i = Index('blog')

            @i.document
            class Post(Document):
                title = Text()

            # create the index, including Post mappings
            i.create()

            # .search() will now return a Search object that will return
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

    def settings(self, **kwargs):
        """
        Add settings to the index::

            i = Index('i')
            i.settings(number_of_shards=1, number_of_replicas=0)

        Multiple calls to ``settings`` will merge the keys, later overriding
        the earlier.
        """
        self._settings.update(kwargs)
        return self

    def aliases(self, **kwargs):
        """
        Add aliases to the index definition::

            i = Index('blog-v2')
            i.aliases(blog={}, published={'filter': Q('term', published=True)})
        """
        self._aliases.update(kwargs)
        return self

    def analyzer(self, *args, **kwargs):
        """
        Explicitly add an analyzer to an index. Note that all custom analyzers
        defined in mappings will also be created. This is useful for search analyzers.

        Example::

            from elasticsearch_dsl import analyzer, tokenizer

            my_analyzer = analyzer('my_analyzer',
                tokenizer=tokenizer('trigram', 'nGram', min_gram=3, max_gram=3),
                filter=['lowercase']
            )

            i = Index('blog')
            i.analyzer(my_analyzer)

        """
        analyzer = analysis.analyzer(*args, **kwargs)
        d = analyzer.get_analysis_definition()
        # empty custom analyzer, probably already defined out of our control
        if not d:
            return

        # merge the definition
        merge(self._analysis, d, True)

    def to_dict(self):
        out = {}
        if self._settings:
            out["settings"] = self._settings
        if self._aliases:
            out["aliases"] = self._aliases
        mappings = self._mapping.to_dict() if self._mapping else {}
        analysis = self._mapping._collect_analysis() if self._mapping else {}
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

    def search(self, using=None):
        """
        Return a :class:`~elasticsearch_dsl.Search` object searching over the
        index (or all the indices belonging to this template) and its
        ``Document``\\s.
        """
        return Search(
            using=using or self._using, index=self._name, doc_type=self._doc_types
        )

    def updateByQuery(self, using=None):
        """
        Return a :class:`~elasticsearch_dsl.UpdateByQuery` object searching over the index
        (or all the indices belonging to this template) and updating Documents that match
        the search criteria.

        For more information, see here:
        https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
        """
        return UpdateByQuery(
            using=using or self._using,
            index=self._name,
        )

    def create(self, using=None, **kwargs):
        """
        Creates the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.create`` unchanged.
        """
        es = self._get_connection(using)

        if CLIENT_HAS_NAMED_BODY_PARAMS:
            params = self.to_dict()
        else:
            params = {"body": self.to_dict()}
        params.update(kwargs)

        return es.indices.create(index=self._name, **params)

    def is_closed(self, using=None):
        state = self._get_connection(using).cluster.state(
            index=self._name, metric="metadata"
        )
        return state["metadata"]["indices"][self._name]["state"] == "close"

    def save(self, using=None):
        """
        Sync the index definition with elasticsearch, creating the index if it
        doesn't exist and updating its settings and mappings if it does.

        Note some settings and mapping changes cannot be done on an open
        index (or at all on an existing index) and for those this method will
        fail with the underlying exception.
        """
        if not self.exists(using=using):
            return self.create(using=using)

        body = self.to_dict()
        settings = body.pop("settings", {})
        analysis = settings.pop("analysis", None)
        current_settings = self.get_settings(using=using)[self._name]["settings"][
            "index"
        ]
        if analysis:
            if self.is_closed(using=using):
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
                self.put_settings(using=using, body=settings)

        # update the mappings, any conflict in the mappings will result in an
        # exception
        mappings = body.pop("mappings", {})
        if mappings:
            self.put_mapping(using=using, body=mappings)

    def analyze(self, using=None, **kwargs):
        """
        Perform the analysis process on a text and return the tokens breakdown
        of the text.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.analyze`` unchanged.
        """
        return self._get_connection(using).indices.analyze(index=self._name, **kwargs)

    def refresh(self, using=None, **kwargs):
        """
        Performs a refresh operation on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.refresh`` unchanged.
        """
        return self._get_connection(using).indices.refresh(index=self._name, **kwargs)

    def flush(self, using=None, **kwargs):
        """
        Performs a flush operation on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.flush`` unchanged.
        """
        return self._get_connection(using).indices.flush(index=self._name, **kwargs)

    def get(self, using=None, **kwargs):
        """
        The get index API allows to retrieve information about the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get`` unchanged.
        """
        return self._get_connection(using).indices.get(index=self._name, **kwargs)

    def open(self, using=None, **kwargs):
        """
        Opens the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.open`` unchanged.
        """
        return self._get_connection(using).indices.open(index=self._name, **kwargs)

    def close(self, using=None, **kwargs):
        """
        Closes the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.close`` unchanged.
        """
        return self._get_connection(using).indices.close(index=self._name, **kwargs)

    def delete(self, using=None, **kwargs):
        """
        Deletes the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.delete`` unchanged.
        """
        return self._get_connection(using).indices.delete(index=self._name, **kwargs)

    def exists(self, using=None, **kwargs):
        """
        Returns ``True`` if the index already exists in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.exists`` unchanged.
        """
        return self._get_connection(using).indices.exists(index=self._name, **kwargs)

    def exists_type(self, using=None, **kwargs):
        """
        Check if a type/types exists in the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.exists_type`` unchanged.
        """
        return self._get_connection(using).indices.exists_type(
            index=self._name, **kwargs
        )

    def put_mapping(self, using=None, **kwargs):
        """
        Register specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_mapping`` unchanged.
        """
        return self._get_connection(using).indices.put_mapping(
            index=self._name, **kwargs
        )

    def get_mapping(self, using=None, **kwargs):
        """
        Retrieve specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_mapping`` unchanged.
        """
        return self._get_connection(using).indices.get_mapping(
            index=self._name, **kwargs
        )

    def get_field_mapping(self, using=None, **kwargs):
        """
        Retrieve mapping definition of a specific field.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_field_mapping`` unchanged.
        """
        return self._get_connection(using).indices.get_field_mapping(
            index=self._name, **kwargs
        )

    def put_alias(self, using=None, **kwargs):
        """
        Create an alias for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_alias`` unchanged.
        """
        return self._get_connection(using).indices.put_alias(index=self._name, **kwargs)

    def exists_alias(self, using=None, **kwargs):
        """
        Return a boolean indicating whether given alias exists for this index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.exists_alias`` unchanged.
        """
        return self._get_connection(using).indices.exists_alias(
            index=self._name, **kwargs
        )

    def get_alias(self, using=None, **kwargs):
        """
        Retrieve a specified alias.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_alias`` unchanged.
        """
        return self._get_connection(using).indices.get_alias(index=self._name, **kwargs)

    def delete_alias(self, using=None, **kwargs):
        """
        Delete specific alias.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.delete_alias`` unchanged.
        """
        return self._get_connection(using).indices.delete_alias(
            index=self._name, **kwargs
        )

    def get_settings(self, using=None, **kwargs):
        """
        Retrieve settings for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_settings`` unchanged.
        """
        return self._get_connection(using).indices.get_settings(
            index=self._name, **kwargs
        )

    def put_settings(self, using=None, **kwargs):
        """
        Change specific index level settings in real time.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_settings`` unchanged.
        """
        return self._get_connection(using).indices.put_settings(
            index=self._name, **kwargs
        )

    def stats(self, using=None, **kwargs):
        """
        Retrieve statistics on different operations happening on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.stats`` unchanged.
        """
        return self._get_connection(using).indices.stats(index=self._name, **kwargs)

    def segments(self, using=None, **kwargs):
        """
        Provide low level segments information that a Lucene index (shard
        level) is built with.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.segments`` unchanged.
        """
        return self._get_connection(using).indices.segments(index=self._name, **kwargs)

    def validate_query(self, using=None, **kwargs):
        """
        Validate a potentially expensive query without executing it.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.validate_query`` unchanged.
        """
        return self._get_connection(using).indices.validate_query(
            index=self._name, **kwargs
        )

    def clear_cache(self, using=None, **kwargs):
        """
        Clear all caches or specific cached associated with the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.clear_cache`` unchanged.
        """
        return self._get_connection(using).indices.clear_cache(
            index=self._name, **kwargs
        )

    def recovery(self, using=None, **kwargs):
        """
        The indices recovery API provides insight into on-going shard
        recoveries for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.recovery`` unchanged.
        """
        return self._get_connection(using).indices.recovery(index=self._name, **kwargs)

    def upgrade(self, using=None, **kwargs):
        """
        Upgrade the index to the latest format.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.upgrade`` unchanged.
        """
        return self._get_connection(using).indices.upgrade(index=self._name, **kwargs)

    def get_upgrade(self, using=None, **kwargs):
        """
        Monitor how much of the index is upgraded.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_upgrade`` unchanged.
        """
        return self._get_connection(using).indices.get_upgrade(
            index=self._name, **kwargs
        )

    def flush_synced(self, using=None, **kwargs):
        """
        Perform a normal flush, then add a generated unique marker (sync_id) to
        all shards.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.flush_synced`` unchanged.
        """
        return self._get_connection(using).indices.flush_synced(
            index=self._name, **kwargs
        )

    def shard_stores(self, using=None, **kwargs):
        """
        Provides store information for shard copies of the index. Store
        information reports on which nodes shard copies exist, the shard copy
        version, indicating how recent they are, and any exceptions encountered
        while opening the shard index or from earlier engine failure.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.shard_stores`` unchanged.
        """
        return self._get_connection(using).indices.shard_stores(
            index=self._name, **kwargs
        )

    def forcemerge(self, using=None, **kwargs):
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
        return self._get_connection(using).indices.forcemerge(
            index=self._name, **kwargs
        )

    def shrink(self, using=None, **kwargs):
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
        return self._get_connection(using).indices.shrink(index=self._name, **kwargs)
