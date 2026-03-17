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

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:
    import collections as collections_abc

from fnmatch import fnmatch

from elasticsearch.exceptions import NotFoundError, RequestError
from six import add_metaclass, iteritems

from .connections import CLIENT_HAS_NAMED_BODY_PARAMS, get_connection
from .exceptions import IllegalOperation, ValidationException
from .field import Field
from .index import Index
from .mapping import Mapping
from .search import Search
from .utils import DOC_META_FIELDS, META_FIELDS, ObjectBase, merge


class MetaField(object):
    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs


class DocumentMeta(type):
    def __new__(cls, name, bases, attrs):
        # DocumentMeta filters attrs in place
        attrs["_doc_type"] = DocumentOptions(name, bases, attrs)
        return super(DocumentMeta, cls).__new__(cls, name, bases, attrs)


class IndexMeta(DocumentMeta):
    # global flag to guard us from associating an Index with the base Document
    # class, only user defined subclasses should have an _index attr
    _document_initialized = False

    def __new__(cls, name, bases, attrs):
        new_cls = super(IndexMeta, cls).__new__(cls, name, bases, attrs)
        if cls._document_initialized:
            index_opts = attrs.pop("Index", None)
            index = cls.construct_index(index_opts, bases)
            new_cls._index = index
            index.document(new_cls)
        cls._document_initialized = True
        return new_cls

    @classmethod
    def construct_index(cls, opts, bases):
        if opts is None:
            for b in bases:
                if hasattr(b, "_index"):
                    return b._index

            # Set None as Index name so it will set _all while making the query
            return Index(name=None)

        i = Index(getattr(opts, "name", "*"), using=getattr(opts, "using", "default"))
        i.settings(**getattr(opts, "settings", {}))
        i.aliases(**getattr(opts, "aliases", {}))
        for a in getattr(opts, "analyzers", ()):
            i.analyzer(a)
        return i


class DocumentOptions(object):
    def __init__(self, name, bases, attrs):
        meta = attrs.pop("Meta", None)

        # create the mapping instance
        self.mapping = getattr(meta, "mapping", Mapping())

        # register all declared fields into the mapping
        for name, value in list(iteritems(attrs)):
            if isinstance(value, Field):
                self.mapping.field(name, value)
                del attrs[name]

        # add all the mappings for meta fields
        for name in dir(meta):
            if isinstance(getattr(meta, name, None), MetaField):
                params = getattr(meta, name)
                self.mapping.meta(name, *params.args, **params.kwargs)

        # document inheritance - include the fields from parents' mappings
        for b in bases:
            if hasattr(b, "_doc_type") and hasattr(b._doc_type, "mapping"):
                self.mapping.update(b._doc_type.mapping, update_only=True)

    @property
    def name(self):
        return self.mapping.properties.name


@add_metaclass(DocumentMeta)
class InnerDoc(ObjectBase):
    """
    Common class for inner documents like Object or Nested
    """

    @classmethod
    def from_es(cls, data, data_only=False):
        if data_only:
            data = {"_source": data}
        return super(InnerDoc, cls).from_es(data)


@add_metaclass(IndexMeta)
class Document(ObjectBase):
    """
    Model-like class for persisting documents in elasticsearch.
    """

    @classmethod
    def _matches(cls, hit):
        if cls._index._name is None:
            return True
        return fnmatch(hit.get("_index", ""), cls._index._name)

    @classmethod
    def _get_using(cls, using=None):
        return using or cls._index._using

    @classmethod
    def _get_connection(cls, using=None):
        return get_connection(cls._get_using(using))

    @classmethod
    def _default_index(cls, index=None):
        return index or cls._index._name

    @classmethod
    def init(cls, index=None, using=None):
        """
        Create the index and populate the mappings in elasticsearch.
        """
        i = cls._index
        if index:
            i = i.clone(name=index)
        i.save(using=using)

    def _get_index(self, index=None, required=True):
        if index is None:
            index = getattr(self.meta, "index", None)
        if index is None:
            index = getattr(self._index, "_name", None)
        if index is None and required:
            raise ValidationException("No index")
        if index and "*" in index:
            raise ValidationException("You cannot write to a wildcard index.")
        return index

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                "{}={!r}".format(key, getattr(self.meta, key))
                for key in ("index", "id")
                if key in self.meta
            ),
        )

    @classmethod
    def search(cls, using=None, index=None):
        """
        Create an :class:`~elasticsearch_dsl.Search` instance that will search
        over this ``Document``.
        """
        return Search(
            using=cls._get_using(using), index=cls._default_index(index), doc_type=[cls]
        )

    @classmethod
    def get(cls, id, using=None, index=None, **kwargs):
        """
        Retrieve a single document from elasticsearch using its ``id``.

        :arg id: ``id`` of the document to be retrieved
        :arg index: elasticsearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``

        Any additional keyword arguments will be passed to
        ``Elasticsearch.get`` unchanged.
        """
        es = cls._get_connection(using)
        doc = es.get(index=cls._default_index(index), id=id, **kwargs)
        if not doc.get("found", False):
            return None
        return cls.from_es(doc)

    @classmethod
    def exists(cls, id, using=None, index=None, **kwargs):
        """
        check if exists a single document from elasticsearch using its ``id``.

        :arg id: ``id`` of the document to check if exists
        :arg index: elasticsearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``

        Any additional keyword arguments will be passed to
        ``Elasticsearch.exists`` unchanged.
        """
        es = cls._get_connection(using)
        return es.exists(index=cls._default_index(index), id=id, **kwargs)

    @classmethod
    def mget(
        cls, docs, using=None, index=None, raise_on_error=True, missing="none", **kwargs
    ):
        r"""
        Retrieve multiple document by their ``id``\s. Returns a list of instances
        in the same order as requested.

        :arg docs: list of ``id``\s of the documents to be retrieved or a list
            of document specifications as per
            https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
        :arg index: elasticsearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``
        :arg missing: what to do when one of the documents requested is not
            found. Valid options are ``'none'`` (use ``None``), ``'raise'`` (raise
            ``NotFoundError``) or ``'skip'`` (ignore the missing document).

        Any additional keyword arguments will be passed to
        ``Elasticsearch.mget`` unchanged.
        """
        if missing not in ("raise", "skip", "none"):
            raise ValueError("'missing' must be 'raise', 'skip', or 'none'.")
        es = cls._get_connection(using)
        body = {
            "docs": [
                doc if isinstance(doc, collections_abc.Mapping) else {"_id": doc}
                for doc in docs
            ]
        }
        results = es.mget(body, index=cls._default_index(index), **kwargs)

        objs, error_docs, missing_docs = [], [], []
        for doc in results["docs"]:
            if doc.get("found"):
                if error_docs or missing_docs:
                    # We're going to raise an exception anyway, so avoid an
                    # expensive call to cls.from_es().
                    continue

                objs.append(cls.from_es(doc))

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
            message = "Documents %s not found." % ", ".join(missing_ids)
            raise NotFoundError(404, message, {"docs": missing_docs})
        return objs

    def delete(self, using=None, index=None, **kwargs):
        """
        Delete the instance in elasticsearch.

        :arg index: elasticsearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``

        Any additional keyword arguments will be passed to
        ``Elasticsearch.delete`` unchanged.
        """
        es = self._get_connection(using)
        # extract routing etc from meta
        doc_meta = {k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        # Optimistic concurrency control
        if "seq_no" in self.meta and "primary_term" in self.meta:
            doc_meta["if_seq_no"] = self.meta["seq_no"]
            doc_meta["if_primary_term"] = self.meta["primary_term"]

        doc_meta.update(kwargs)
        es.delete(index=self._get_index(index), **doc_meta)

    def to_dict(self, include_meta=False, skip_empty=True):
        """
        Serialize the instance into a dictionary so that it can be saved in elasticsearch.

        :arg include_meta: if set to ``True`` will include all the metadata
            (``_index``, ``_id`` etc). Otherwise just the document's
            data is serialized. This is useful when passing multiple instances into
            ``elasticsearch.helpers.bulk``.
        :arg skip_empty: if set to ``False`` will cause empty values (``None``,
            ``[]``, ``{}``) to be left on the document. Those values will be
            stripped out otherwise as they make no difference in elasticsearch.
        """
        d = super(Document, self).to_dict(skip_empty=skip_empty)
        if not include_meta:
            return d

        meta = {"_" + k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        # in case of to_dict include the index unlike save/update/delete
        index = self._get_index(required=False)
        if index is not None:
            meta["_index"] = index

        meta["_source"] = d
        return meta

    def update(
        self,
        using=None,
        index=None,
        detect_noop=True,
        doc_as_upsert=False,
        refresh=False,
        retry_on_conflict=None,
        script=None,
        script_id=None,
        scripted_upsert=False,
        upsert=None,
        return_doc_meta=False,
        **fields
    ):
        """
        Partial update of the document, specify fields you wish to update and
        both the instance and the document in elasticsearch will be updated::

            doc = MyDocument(title='Document Title!')
            doc.save()
            doc.update(title='New Document Title!')

        :arg index: elasticsearch index to use, if the ``Document`` is
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
        body = {
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

            # prepare data for ES
            values = self.to_dict()

            # if fields were given: partial update
            body["doc"] = {k: values.get(k) for k in fields.keys()}

        # extract routing etc from meta
        params = {k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        if retry_on_conflict is not None:
            params["retry_on_conflict"] = retry_on_conflict

        # Optimistic concurrency control
        if (
            retry_on_conflict in (None, 0)
            and "seq_no" in self.meta
            and "primary_term" in self.meta
        ):
            params["if_seq_no"] = self.meta["seq_no"]
            params["if_primary_term"] = self.meta["primary_term"]

        params["refresh"] = refresh

        if CLIENT_HAS_NAMED_BODY_PARAMS:
            params.update(body)
        else:
            params["body"] = body

        meta = self._get_connection(using).update(
            index=self._get_index(index), **params
        )
        # update meta information from ES
        for k in META_FIELDS:
            if "_" + k in meta:
                setattr(self.meta, k, meta["_" + k])

        return meta if return_doc_meta else meta["result"]

    def save(
        self,
        using=None,
        index=None,
        validate=True,
        skip_empty=True,
        return_doc_meta=False,
        **kwargs
    ):
        """
        Save the document into elasticsearch. If the document doesn't exist it
        is created, it is overwritten otherwise. Returns ``True`` if this
        operations resulted in new document being created.

        :arg index: elasticsearch index to use, if the ``Document`` is
            associated with an index this can be omitted.
        :arg using: connection alias to use, defaults to ``'default'``
        :arg validate: set to ``False`` to skip validating the document
        :arg skip_empty: if set to ``False`` will cause empty values (``None``,
            ``[]``, ``{}``) to be left on the document. Those values will be
            stripped out otherwise as they make no difference in elasticsearch.
        :arg return_doc_meta: set to ``True`` to return all metadata from the
            update API call instead of only the operation result

        Any additional keyword arguments will be passed to
        ``Elasticsearch.index`` unchanged.

        :return operation result created/updated
        """
        if validate:
            self.full_clean()

        es = self._get_connection(using)
        # extract routing etc from meta
        params = {k: self.meta[k] for k in DOC_META_FIELDS if k in self.meta}

        # Optimistic concurrency control
        if "seq_no" in self.meta and "primary_term" in self.meta:
            params["if_seq_no"] = self.meta["seq_no"]
            params["if_primary_term"] = self.meta["primary_term"]

        if CLIENT_HAS_NAMED_BODY_PARAMS:
            params["document"] = self.to_dict(skip_empty=skip_empty)
        else:
            params["body"] = self.to_dict(skip_empty=skip_empty)

        params.update(kwargs)
        meta = es.index(index=self._get_index(index), **params)
        # update meta information from ES
        for k in META_FIELDS:
            if "_" + k in meta:
                setattr(self.meta, k, meta["_" + k])

        return meta if return_doc_meta else meta["result"]
