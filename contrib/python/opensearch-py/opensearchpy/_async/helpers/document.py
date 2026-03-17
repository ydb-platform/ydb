# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import collections.abc as collections_abc
from fnmatch import fnmatch
from typing import Any, Optional, Tuple, Type

from opensearchpy._async.client import AsyncOpenSearch
from opensearchpy._async.helpers.index import AsyncIndex
from opensearchpy._async.helpers.search import AsyncSearch
from opensearchpy.connection.async_connections import get_connection
from opensearchpy.exceptions import (
    IllegalOperation,
    NotFoundError,
    RequestError,
    ValidationException,
)
from opensearchpy.helpers.document import DocumentMeta
from opensearchpy.helpers.utils import DOC_META_FIELDS, META_FIELDS, ObjectBase, merge


class AsyncIndexMeta(DocumentMeta):
    # global flag to guard us from associating an Index with the base Document
    # class, only user defined subclasses should have an _index attr
    _document_initialized = False

    def __new__(
        cls,
        name: str,
        bases: Tuple[Type[ObjectBase]],
        attrs: Any,
    ) -> Any:
        new_cls = super().__new__(cls, name, bases, attrs)
        if cls._document_initialized:
            index_opts = attrs.pop("Index", None)
            index = cls.construct_index(index_opts, bases)
            new_cls._index = index
            index.document(new_cls)
        cls._document_initialized = True
        return new_cls

    @classmethod
    def construct_index(cls, opts: Any, bases: Any) -> Any:
        if opts is None:
            for b in bases:
                if hasattr(b, "_index"):
                    return b._index

            # Set None as Index name so it will set _all while making the query
            return AsyncIndex(name=None)

        i = AsyncIndex(
            getattr(opts, "name", "*"), using=getattr(opts, "using", "default")
        )
        i.settings(**getattr(opts, "settings", {}))
        i.aliases(**getattr(opts, "aliases", {}))
        for a in getattr(opts, "analyzers", ()):
            i.analyzer(a)
        return i


class AsyncDocument(ObjectBase, metaclass=AsyncIndexMeta):
    """
    Model-like class for persisting documents in opensearch.
    """

    @classmethod
    def _matches(cls: Any, hit: Any) -> bool:
        if cls._index._name is None:
            return True
        return fnmatch(hit.get("_index", ""), cls._index._name)

    @classmethod
    def _get_using(cls: Any, using: Any = None) -> Any:
        return using or cls._index._using

    @classmethod
    async def _get_connection(cls, using: Optional[AsyncOpenSearch] = None) -> Any:
        return await get_connection(cls._get_using(using))

    @classmethod
    def _default_index(cls: Any, index: Any = None) -> Any:
        return index or cls._index._name

    @classmethod
    async def init(
        cls: Any, index: Optional[str] = None, using: Optional[AsyncOpenSearch] = None
    ) -> None:
        """
        Create the index and populate the mappings in opensearch.
        """
        i = cls._index
        if index:
            i = i.clone(name=index)
        await i.save(using=using)

    def _get_index(
        self, index: Optional[str] = None, required: Optional[bool] = True
    ) -> Any:
        if index is None:
            index = getattr(self.meta, "index", None)
        if index is None:
            index = getattr(self._index, "_name", None)
        if index is None and required:
            raise ValidationException("No index")
        if index and "*" in index:
            raise ValidationException("You cannot write to a wildcard index.")
        return index

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                f"{key}={getattr(self.meta, key)!r}"
                for key in ("index", "id")
                if key in self.meta
            ),
        )

    @classmethod
    def search(cls, using: Any = None, index: Any = None) -> AsyncSearch:
        """
        Create an :class:`~opensearchpy.AsyncSearch` instance that will search
        over this ``Document``.
        """
        return AsyncSearch(
            using=cls._get_using(using), index=cls._default_index(index), doc_type=[cls]
        )

    @classmethod
    async def get(  # type: ignore
        cls,
        id: Any,
        using: Any = None,
        index: Any = None,
        **kwargs: Any,
    ) -> Any:
        """
        Retrieve a single document from opensearch using its ``id``.

        :arg id: ``id`` of the document to be retrieved
        :arg index: opensearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.get`` unchanged.
        """
        opensearch = await cls._get_connection(using)
        doc = await opensearch.get(index=cls._default_index(index), id=id, **kwargs)
        if not doc.get("found", False):
            return None
        return cls.from_opensearch(doc)

    @classmethod
    async def exists(
        cls,
        id: str,
        using: Optional[AsyncOpenSearch] = None,
        index: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """
        check if exists a single document from opensearch using its ``id``.

        :arg id: ``id`` of the document to check if exists
        :arg index: opensearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.exists`` unchanged.
        """
        opensearch = await cls._get_connection(using)
        return await opensearch.exists(index=cls._default_index(index), id=id, **kwargs)

    @classmethod
    async def mget(
        cls,
        docs: Any,
        using: Optional[AsyncOpenSearch] = None,
        index: Optional[str] = None,
        raise_on_error: Optional[bool] = True,
        missing: Optional[str] = "none",
        **kwargs: Any,
    ) -> Any:
        """
        Retrieve multiple document by their ``id``'s. Returns a list of instances
        in the same order as requested.

        :arg docs: list of ``id``'s of the documents to be retrieved or a list
            of document specifications as per
            https://opensearch.org/docs/latest/opensearch/rest-api/document-apis/multi-get/
        :arg index: opensearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``
        :arg missing: what to do when one of the documents requested is not
            found. Valid options are ``'none'`` (use ``None``), ``'raise'`` (raise
            ``NotFoundError``) or ``'skip'`` (ignore the missing document).

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.mget`` unchanged.
        """
        if missing not in ("raise", "skip", "none"):
            raise ValueError("'missing' must be 'raise', 'skip', or 'none'.")
        opensearch = await cls._get_connection(using)
        body = {
            "docs": [
                doc if isinstance(doc, collections_abc.Mapping) else {"_id": doc}
                for doc in docs
            ]
        }
        results = await opensearch.mget(
            body=body, index=cls._default_index(index), **kwargs
        )

        objs: Any = []
        error_docs: Any = []
        missing_docs: Any = []
        for doc in results["docs"]:
            if doc.get("found"):
                if error_docs or missing_docs:
                    # We're going to raise an exception anyway, so avoid an
                    # expensive call to cls.from_opensearch().
                    continue

                objs.append(cls.from_opensearch(doc))

            elif doc.get("error"):
                if raise_on_error:
                    error_docs.append(doc)
                if missing == "none":
                    objs.append(None)

            # The doc didn't cause an error, but the doc also wasn't found.
            elif missing == "raise":
                missing_docs.append(doc)
            elif missing == "none":
                objs.append(None)

        if error_docs:
            error_ids = [doc["_id"] for doc in error_docs]
            message = "Required routing not provided for documents %s."
            message %= ", ".join(error_ids)
            raise RequestError(400, message, error_docs)
        if missing_docs:
            missing_ids = [doc["_id"] for doc in missing_docs]
            message = f"Documents {', '.join(missing_ids)} not found."
            raise NotFoundError(404, message, {"docs": missing_docs})
        return objs

    async def delete(
        self,
        using: Optional[AsyncOpenSearch] = None,
        index: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Delete the instance in opensearch.

        :arg index: opensearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.delete`` unchanged.
        """
        opensearch = await self._get_connection(using)
        # extract routing etc from meta
        doc_meta = {k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        # Optimistic concurrency control
        if "seq_no" in self.meta and "primary_term" in self.meta:
            doc_meta["if_seq_no"] = self.meta["seq_no"]
            doc_meta["if_primary_term"] = self.meta["primary_term"]

        doc_meta.update(kwargs)
        await opensearch.delete(index=self._get_index(index), **doc_meta)

    def to_dict(  # type: ignore
        self, include_meta: Optional[bool] = False, skip_empty: Optional[bool] = True
    ) -> Any:
        """
        Serialize the instance into a dictionary so that it can be saved in opensearch.

        :arg include_meta: if set to ``True`` will include all the metadata
            (``_index``, ``_id`` etc). Otherwise just the document's
            data is serialized.
        :arg skip_empty: if set to ``False`` will cause empty values (``None``,
            ``[]``, ``{}``) to be left on the document. Those values will be
            stripped out otherwise as they make no difference in opensearch.
        """
        d = super().to_dict(skip_empty)
        if not include_meta:
            return d

        meta = {"_" + k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        # in case of to_dict include the index unlike save/update/delete
        index = self._get_index(required=False)
        if index is not None:
            meta["_index"] = index

        meta["_source"] = d
        return meta

    async def update(
        self,
        using: Optional[AsyncOpenSearch] = None,
        index: Optional[str] = None,
        detect_noop: Optional[bool] = True,
        doc_as_upsert: Optional[bool] = False,
        refresh: Optional[bool] = False,
        retry_on_conflict: Optional[int] = None,
        script: Any = None,
        script_id: Optional[str] = None,
        scripted_upsert: Optional[bool] = False,
        upsert: Optional[bool] = None,
        return_doc_meta: Optional[bool] = False,
        **fields: Any,
    ) -> Any:
        """
        Partial update of the document, specify fields you wish to update and
        both the instance and the document in opensearch will be updated::

            doc = MyDocument(title='Document Title!')
            doc.save()
            doc.update(title='New Document Title!')

        :arg index: opensearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``
        :arg detect_noop: Set to ``False`` to disable noop detection.
        :arg refresh: Control when the changes made by this request are visible
            to search. Set to ``True`` for immediate effect.
        :arg retry_on_conflict: In between the get and indexing phases of the
            update, it is possible that another process might have already
            updated the same document. By default, the update will fail with a
            version conflict exception. The retry_on_conflict parameter
            controls how many times to retry the update before finally throwing
            an exception.
        :arg doc_as_upsert:  Instead of sending a partial doc plus an upsert
            doc, setting doc_as_upsert to true will use the contents of doc as
            the upsert value
        :arg return_doc_meta: set to ``True`` to return all metadata from the
            index API call instead of only the operation result

        :return operation result noop/updated
        """
        body: Any = {
            "doc_as_upsert": doc_as_upsert,
            "detect_noop": detect_noop,
        }

        # scripted update
        if script or script_id:
            if upsert is not None:
                body["upsert"] = upsert

            if script:
                script = {"source": script}
            else:
                script = {"id": script_id}

            script["params"] = fields

            body["script"] = script
            body["scripted_upsert"] = scripted_upsert

        # partial document update
        else:
            if not fields:
                raise IllegalOperation(
                    "You cannot call update() without updating individual fields or a script. "
                    "If you wish to update the entire object use save()."
                )

            # update given fields locally
            merge(self, fields)

            # prepare data for OpenSearch
            values = self.to_dict()

            # if fields were given: partial update
            body["doc"] = {k: values.get(k) for k in fields.keys()}

        # extract routing etc from meta
        doc_meta = {k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        if retry_on_conflict is not None:
            doc_meta["retry_on_conflict"] = retry_on_conflict

        # Optimistic concurrency control
        if (
            retry_on_conflict in (None, 0)
            and "seq_no" in self.meta
            and "primary_term" in self.meta
        ):
            doc_meta["if_seq_no"] = self.meta["seq_no"]
            doc_meta["if_primary_term"] = self.meta["primary_term"]

        meta = await (await self._get_connection(using)).update(
            index=self._get_index(index), body=body, refresh=refresh, **doc_meta
        )
        # update meta information from OpenSearch
        for k in META_FIELDS:
            if "_" + k in meta:
                setattr(self.meta, k, meta["_" + k])

        return meta if return_doc_meta else meta["result"]

    async def save(
        self,
        using: Optional[AsyncOpenSearch] = None,
        index: Optional[str] = None,
        validate: Optional[bool] = True,
        skip_empty: Optional[bool] = True,
        return_doc_meta: Optional[bool] = False,
        **kwargs: Any,
    ) -> Any:
        """
        Save the document into opensearch. If the document doesn't exist it
        is created, it is overwritten otherwise. Returns ``True`` if this
        operations resulted in new document being created.

        :arg index: opensearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``
        :arg validate: set to ``False`` to skip validating the document
        :arg skip_empty: if set to ``False`` will cause empty values (``None``,
            ``[]``, ``{}``) to be left on the document. Those values will be
            stripped out otherwise as they make no difference in opensearch.
        :arg return_doc_meta: set to ``True`` to return all metadata from the
            update API call instead of only the operation result

        Any additional keyword arguments will be passed to
        ``AsyncOpenSearch.index`` unchanged.

        :return operation result created/updated
        """
        if validate:
            self.full_clean()

        opensearch = await self._get_connection(using)
        # extract routing etc from meta
        doc_meta = {k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        # Optimistic concurrency control
        if "seq_no" in self.meta and "primary_term" in self.meta:
            doc_meta["if_seq_no"] = self.meta["seq_no"]
            doc_meta["if_primary_term"] = self.meta["primary_term"]

        doc_meta.update(kwargs)
        meta = await opensearch.index(
            index=self._get_index(index),
            body=self.to_dict(skip_empty=skip_empty),
            **doc_meta,
        )
        # update meta information from OpenSearch
        for k in META_FIELDS:
            if "_" + k in meta:
                setattr(self.meta, k, meta["_" + k])

        return meta if return_doc_meta else meta["result"]
