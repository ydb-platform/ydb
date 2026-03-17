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

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from typing_extensions import Self

from . import analysis
from .utils import AnyUsingType, merge

if TYPE_CHECKING:
    from .document_base import DocumentMeta
    from .field import Field
    from .mapping_base import MappingBase


class IndexBase:
    def __init__(self, name: str, mapping_class: type, using: AnyUsingType = "default"):
        """
        :arg name: name of the index
        :arg using: connection alias to use, defaults to ``'default'``
        """
        self._name = name
        self._doc_types: List["DocumentMeta"] = []
        self._using = using
        self._settings: Dict[str, Any] = {}
        self._aliases: Dict[str, Any] = {}
        self._analysis: Dict[str, Any] = {}
        self._mapping_class = mapping_class
        self._mapping: Optional["MappingBase"] = None

    def resolve_nested(
        self, field_path: str
    ) -> Tuple[List[str], Optional["MappingBase"]]:
        for doc in self._doc_types:
            nested, field = doc._doc_type.mapping.resolve_nested(field_path)
            if field is not None:
                return nested, field
        if self._mapping:
            return self._mapping.resolve_nested(field_path)
        return [], None

    def resolve_field(self, field_path: str) -> Optional["Field"]:
        for doc in self._doc_types:
            field = doc._doc_type.mapping.resolve_field(field_path)
            if field is not None:
                return field
        if self._mapping:
            return self._mapping.resolve_field(field_path)
        return None

    def get_or_create_mapping(self) -> "MappingBase":
        if self._mapping is None:
            self._mapping = self._mapping_class()
        return self._mapping

    def mapping(self, mapping: "MappingBase") -> None:
        """
        Associate a mapping (an instance of
        :class:`~elasticsearch.dsl.Mapping`) with this index.
        This means that, when this index is created, it will contain the
        mappings for the document type defined by those mappings.
        """
        self.get_or_create_mapping().update(mapping)

    def document(self, document: "DocumentMeta") -> "DocumentMeta":
        """
        Associate a :class:`~elasticsearch.dsl.Document` subclass with an index.
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

    def settings(self, **kwargs: Any) -> Self:
        """
        Add settings to the index::

            i = Index('i')
            i.settings(number_of_shards=1, number_of_replicas=0)

        Multiple calls to ``settings`` will merge the keys, later overriding
        the earlier.
        """
        self._settings.update(kwargs)
        return self

    def aliases(self, **kwargs: Any) -> Self:
        """
        Add aliases to the index definition::

            i = Index('blog-v2')
            i.aliases(blog={}, published={'filter': Q('term', published=True)})
        """
        self._aliases.update(kwargs)
        return self

    def analyzer(self, *args: Any, **kwargs: Any) -> None:
        """
        Explicitly add an analyzer to an index. Note that all custom analyzers
        defined in mappings will also be created. This is useful for search analyzers.

        Example::

            from elasticsearch.dsl import analyzer, tokenizer

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

    def to_dict(self) -> Dict[str, Any]:
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
