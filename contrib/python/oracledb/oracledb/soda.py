# -----------------------------------------------------------------------------
# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# soda.py
#
# Contains the classes for managing Simple Oracle Document Access (SODA):
# SodaDatabase, SodaCollection, SodaDocument, SodaDocCursor and SodaOperation.
# -----------------------------------------------------------------------------

from typing import Any, Union, List
import json

from . import errors


class SodaDatabase:
    def __repr__(self):
        return f"<oracledb.SodaDatabase on {self._conn!r}>"

    @classmethod
    def _from_impl(cls, conn, impl):
        db = cls.__new__(cls)
        db._conn = conn
        db._impl = impl
        return db

    def _create_doc_impl(
        self, content: Any, key: str = None, media_type: str = None
    ) -> "SodaDocument":
        """
        Internal method used for creating a document implementation object with
        the given content, key and media type.
        """
        if isinstance(content, str):
            content_bytes = content.encode()
        elif isinstance(content, bytes):
            content_bytes = content
        elif self._impl.supports_json:
            return self._impl.create_json_document(content, key)
        else:
            content_bytes = json.dumps(content).encode()
        return self._impl.create_document(content_bytes, key, media_type)

    def createCollection(
        self,
        name: str,
        metadata: Union[str, dict] = None,
        mapMode: bool = False,
    ) -> "SodaCollection":
        """
        Creates a SODA collection with the given name and returns a new SODA
        collection object. If you try to create a collection, and a collection
        with the same name and metadata already exists, then that existing
        collection is opened without error.

        If metadata is specified, it is expected to be a string containing
        valid JSON or a dictionary that will be transformed into a JSON string.
        This JSON permits you to specify the configuration of the collection
        including storage options; specifying the presence or absence of
        columns for creation timestamp, last modified timestamp and version;
        whether the collection can store only JSON documents; and methods of
        key and version generation. The default metadata creates a collection
        that only supports JSON documents and uses system generated keys.

        If the mapMode parameter is set to True, the new collection is mapped
        to an existing table instead of creating a table. If a collection is
        created in this way, dropping the collection will not drop the existing
        table either.
        """
        if metadata is not None and not isinstance(metadata, str):
            metadata = json.dumps(metadata)
        collection_impl = self._impl.create_collection(name, metadata, mapMode)
        return SodaCollection._from_impl(self, collection_impl)

    def createDocument(
        self,
        content: Any,
        key: str = None,
        mediaType: str = "application/json",
    ) -> "SodaDocument":
        """
        Creates a SODA document usable for SODA write operations. You only need
        to use this method if your collection requires client-assigned keys or
        has non-JSON content; otherwise, you can pass your content directly to
        SODA write operations. SodaDocument attributes "createdOn",
        "lastModified" and "version" will be None.

        The content parameter can be a dictionary or list which will be
        transformed into a JSON string and then UTF-8 encoded. It can also be a
        string which will be UTF-8 encoded or it can be a bytes object which
        will be stored unchanged. If a bytes object is provided and the content
        is expected to be JSON, note that SODA only supports UTF-8, UTF-16LE
        and UTF-16BE encodings.

        The key parameter should only be supplied if the collection in which
        the document is to be placed requires client-assigned keys.

        The mediaType parameter should only be supplied if the collection in
        which the document is to be placed supports non-JSON documents and the
        content for this document is non-JSON. Using a standard MIME type for
        this value is recommended but any string will be accepted.
        """
        doc_impl = self._create_doc_impl(content, key, mediaType)
        return SodaDocument._from_impl(doc_impl)

    def getCollectionNames(
        self, startName: str = None, limit: int = 0
    ) -> List[str]:
        """
        Returns a list of the names of collections in the database that match
        the criteria, in alphabetical order.

        If the startName parameter is specified, the list of names returned
        will start with this value and also contain any names that fall after
        this value in alphabetical order.

        If the limit parameter is specified and is non-zero, the number of
        collection names returned will be limited to this value.
        """
        return self._impl.get_collection_names(startName, limit)

    def openCollection(self, name: str) -> "SodaCollection":
        """
        Opens an existing collection with the given name and returns a new SODA
        collection object. If a collection with that name does not exist, None
        is returned.
        """
        collection_impl = self._impl.open_collection(name)
        if collection_impl is not None:
            return SodaCollection._from_impl(self, collection_impl)


class SodaCollection:
    @classmethod
    def _from_impl(cls, db, impl):
        coll = cls.__new__(cls)
        coll._db = db
        coll._impl = impl
        return coll

    def _process_doc_arg(self, arg):
        if isinstance(arg, SodaDocument):
            return arg._impl
        return self._db._create_doc_impl(arg)

    def createIndex(self, spec: Union[dict, str]) -> None:
        """
        Creates an index on a SODA collection. The spec is expected to be a
        dictionary or a JSON-encoded string.

        Note that a commit should be performed before attempting to create an
        index.
        """
        if isinstance(spec, dict):
            spec = json.dumps(spec)
        elif not isinstance(spec, str):
            raise TypeError("expecting a dictionary or string")
        self._impl.create_index(spec)

    def drop(self) -> bool:
        """
        Drops the collection from the database, if it exists. Note that if the
        collection was created with mapMode set to True the underlying table
        will not be dropped.

        A boolean value is returned indicating if the collection was actually
        dropped.
        """
        return self._impl.drop()

    def dropIndex(self, name: str, force: bool = False) -> bool:
        """
        Drops the index with the specified name, if it exists.

        The force parameter, if set to True, can be used to force the dropping
        of an index that the underlying Oracle Database domain index doesn’t
        normally permit. This is only applicable to spatial and JSON search
        indexes. See here for more information.

        A boolean value is returned indicating if the index was actually
        dropped.
        """
        return self._impl.drop_index(name, force)

    def find(self) -> "SodaOperation":
        """
        This method is used to begin an operation that will act upon documents
        in the collection. It creates and returns a SodaOperation object which
        is used to specify the criteria and the operation that will be
        performed on the documents that match that criteria.
        """
        return SodaOperation(self)

    def getDataGuide(self) -> "SodaDocument":
        """
        Returns a SODA document object containing property names, data types
        and lengths inferred from the JSON documents in the collection. It can
        be useful for exploring the schema of a collection. Note that this
        method is only supported for JSON-only collections where a JSON search
        index has been created with the ‘dataguide’ option enabled. If there
        are no documents in the collection, None is returned.
        """
        doc_impl = self._impl.get_data_guide()
        if doc_impl is not None:
            return SodaDocument._from_impl(doc_impl)

    def insertMany(self, docs: list) -> None:
        """
        Inserts a list of documents into the collection at one time. Each of
        the input documents can be a dictionary or list or an existing SODA
        document object.
        """
        doc_impls = [self._process_doc_arg(d) for d in docs]
        self._impl.insert_many(doc_impls, hint=None, return_docs=False)

    def insertManyAndGet(self, docs: list, hint: str = None) -> list:
        """
        Similarly to insertMany() this method inserts a list of documents into
        the collection at one time. The only difference is that it returns a
        list of SODA Document objects. Note that for performance reasons the
        returned documents do not contain the content.

        The hint parameter, if specified, supplies a hint to the database when
        processing the SODA operation. This is expected to be a string in the
        same format as SQL hints but without any comment characters, for
        example hint="MONITOR". While you could use this to pass any SQL hint,
        the hints MONITOR (turn on monitoring) and NO_MONITOR (turn off
        monitoring) are the most useful. Use of the hint parameter requires
        Oracle Client 21.3 or higher (or Oracle Client 19 from 19.11).
        """
        doc_impls = [self._process_doc_arg(d) for d in docs]
        if hint is not None and not isinstance(hint, str):
            raise TypeError("expecting a string")
        return_doc_impls = self._impl.insert_many(
            doc_impls, hint, return_docs=True
        )
        return [SodaDocument._from_impl(i) for i in return_doc_impls]

    def insertOne(self, doc: Any) -> None:
        """
        Inserts a given document into the collection. The input document can be
        a dictionary or list or an existing SODA document object.
        """
        doc_impl = self._process_doc_arg(doc)
        self._impl.insert_one(doc_impl, hint=None, return_doc=False)

    def insertOneAndGet(self, doc: Any, hint: str = None) -> "SodaDocument":
        """
        Similarly to insertOne() this method inserts a given document into the
        collection. The only difference is that it returns a SODA Document
        object. Note that for performance reasons the returned document does
        not contain the content.

        The hint parameter, if specified, supplies a hint to the database when
        processing the SODA operation. This is expected to be a string in the
        same format as SQL hints but without any comment characters, for
        example hint="MONITOR". While you could use this to pass any SQL hint,
        the hints MONITOR (turn on monitoring) and NO_MONITOR (turn off
        monitoring) are the most useful. Use of the hint parameter requires
        Oracle Client 21.3 or higher (or Oracle Client 19 from 19.11).
        """
        doc_impl = self._process_doc_arg(doc)
        if hint is not None and not isinstance(hint, str):
            raise TypeError("expecting a string")
        return_doc_impl = self._impl.insert_one(
            doc_impl, hint, return_doc=True
        )
        return SodaDocument._from_impl(return_doc_impl)

    def listIndexes(self) -> list:
        """
        Return a list of indexes associated with the collection.
        """
        return [json.loads(s) for s in self._impl.list_indexes()]

    @property
    def metadata(self) -> dict:
        """
        This read-only attribute returns a dictionary containing the metadata
        that was used to create the collection.
        """
        return json.loads(self._impl.get_metadata())

    @property
    def name(self) -> str:
        """
        This read-only attribute returns the name of the collection.
        """
        return self._impl.name

    def save(self, doc: Any) -> None:
        """
        Saves a document into the collection. This method is equivalent to
        insertOne() except that if client-assigned keys are used, and the
        document with the specified key already exists in the collection, it
        will be replaced with the input document.
        """
        doc_impl = self._process_doc_arg(doc)
        self._impl.save(doc_impl, hint=None, return_doc=False)

    def saveAndGet(self, doc: Any, hint: str = None) -> "SodaDocument":
        """
        Saves a document into the collection. This method is equivalent to
        insertOneAndGet() except that if client-assigned keys are used, and the
        document with the specified key already exists in the collection, it
        will be replaced with the input document.

        The hint parameter, if specified, supplies a hint to the database when
        processing the SODA operation. This is expected to be a string in the
        same format as SQL hints but without any comment characters, for
        example hint="MONITOR". While you could use this to pass any SQL hint,
        the hints MONITOR (turn on monitoring) and NO_MONITOR (turn off
        monitoring) are the most useful. Use of the hint parameter requires
        Oracle Client 21.3 or higher (or Oracle Client 19 from 19.11).
        """
        doc_impl = self._process_doc_arg(doc)
        if hint is not None and not isinstance(hint, str):
            raise TypeError("expecting a string")
        return_doc_impl = self._impl.save(doc_impl, hint, return_doc=True)
        return SodaDocument._from_impl(return_doc_impl)

    def truncate(self) -> None:
        """
        Removes all of the documents in the collection, similarly to what is
        done for rows in a table by the TRUNCATE TABLE statement.
        """
        self._impl.truncate()


class SodaDocument:
    @classmethod
    def _from_impl(cls, impl):
        doc = cls.__new__(cls)
        doc._impl = impl
        return doc

    @property
    def createdOn(self) -> str:
        """
        This read-only attribute returns the creation time of the document in
        ISO 8601 format. Documents created by SodaDatabase.createDocument() or
        fetched from collections where this attribute is not stored will return
        None.
        """
        return self._impl.get_created_on()

    def getContent(self) -> Union[dict, list]:
        """
        Returns the content of the document as a dictionary or list. This
        method assumes that the content is application/json and will raise an
        exception if this is not the case. If there is no content, however,
        None will be returned.
        """
        content, encoding = self._impl.get_content()
        if isinstance(content, bytes) and self.mediaType == "application/json":
            return json.loads(content.decode(encoding))
        return content

    def getContentAsBytes(self) -> bytes:
        """
        Returns the content of the document as a bytes object. If there is no
        content, however, None will be returned.
        """
        content, encoding = self._impl.get_content()
        if isinstance(content, bytes):
            return content
        elif content is not None:
            return str(content).encode()

    def getContentAsString(self) -> str:
        """
        Returns the content of the document as a string. If the document
        encoding is not known, UTF-8 will be used. If there is no content,
        however, None will be returned.
        """
        content, encoding = self._impl.get_content()
        if isinstance(content, bytes):
            return content.decode(encoding)
        elif content is not None:
            return str(content)

    @property
    def key(self) -> str:
        """
        This read-only attribute returns the unique key assigned to this
        document. Documents created by SodaDatabase.createDocument() may not
        have a value assigned to them and return None.
        """
        return self._impl.get_key()

    @property
    def lastModified(self) -> str:
        """
        This read-only attribute returns the last modified time of the document
        in ISO 8601 format. Documents created by SodaDatabase.createDocument()
        or fetched from collections where this attribute is not stored will
        return None.
        """
        return self._impl.get_last_modified()

    @property
    def mediaType(self) -> str:
        """
        This read-only attribute returns the media type assigned to the
        document. By convention this is expected to be a MIME type but no
        checks are performed on this value. If a value is not specified when
        calling SodaDatabase.createDocument() or the document is fetched from a
        collection where this component is not stored, the string
        “application/json” is returned.
        """
        return self._impl.get_media_type()

    @property
    def version(self) -> str:
        """
        This read-only attribute returns the version assigned to this document.
        Documents created by SodaDatabase.createDocument() or fetched from
        collections where this attribute is not stored will return None.
        """
        return self._impl.get_version()


class SodaDocCursor:
    def __iter__(self):
        return self

    def __next__(self):
        if self._impl is None:
            errors._raise_err(errors.ERR_CURSOR_NOT_OPEN)
        doc_impl = self._impl.get_next_doc()
        if doc_impl is not None:
            return SodaDocument._from_impl(doc_impl)
        raise StopIteration

    @classmethod
    def _from_impl(cls, impl):
        cursor = cls.__new__(cls)
        cursor._impl = impl
        return cursor

    def close(self) -> None:
        """
        Close the cursor now, rather than whenever __del__ is called. The
        cursor will be unusable from this point forward; an Error exception
        will be raised if any operation is attempted with the cursor.
        """
        if self._impl is None:
            errors._raise_err(errors.ERR_CURSOR_NOT_OPEN)
        self._impl.close()
        self._impl = None


class SodaOperation:
    def __init__(self, collection: SodaCollection) -> None:
        self._collection = collection
        self._key = None
        self._keys = None
        self._version = None
        self._filter = None
        self._hint = None
        self._skip = None
        self._limit = None
        self._fetch_array_size = None
        self._lock = False

    def count(self) -> int:
        """
        Returns a count of the number of documents in the collection that match
        the criteria. If skip() or limit() were called on this object, an
        exception is raised.
        """
        return self._collection._impl.get_count(self)

    def fetchArraySize(self, value: int) -> "SodaOperation":
        """
        This is a tuning method to specify the number of documents that are
        internally fetched in batches by calls to getCursor() and
        getDocuments(). It does not affect how many documents are returned to
        the application. A value of 0 will use the default value (100). This
        method is only available in Oracle Client 19.5 and higher.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if not isinstance(value, int) or value < 0:
            raise TypeError("expecting integer >= 0")
        if value == 0:
            self._fetch_array_size = None
        else:
            self._fetch_array_size = value
        return self

    def filter(self, value: Union[dict, str]) -> "SodaOperation":
        """
        Sets a filter specification for complex document queries and ordering
        of JSON documents. Filter specifications must be provided as a
        dictionary or JSON-encoded string and can include comparisons, regular
        expressions, logical and spatial operators, among others. See the
        overview of SODA filter specifications for more information.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if isinstance(value, dict):
            self._filter = json.dumps(value)
        elif isinstance(value, str):
            self._filter = value
        else:
            raise TypeError("expecting string or dictionary")
        return self

    def getCursor(self) -> "SodaDocCursor":
        """
        Returns a SodaDocCursor object that can be used to iterate over the
        documents that match the criteria.
        """
        impl = self._collection._impl.get_cursor(self)
        return SodaDocCursor._from_impl(impl)

    def getDocuments(self) -> list:
        """
        Returns a list of SodaDocument objects that match the criteria.
        """
        return [d for d in self.getCursor()]

    def getOne(self) -> Union["SodaDocument", None]:
        """
        Returns a single SodaDocument object that matches the criteria. Note
        that if multiple documents match the criteria only the first one is
        returned.
        """
        doc_impl = self._collection._impl.get_one(self)
        if doc_impl is not None:
            return SodaDocument._from_impl(doc_impl)

    def hint(self, value: str) -> "SodaOperation":
        """
        Specifies a hint that will be provided to the SODA operation when it is
        performed. This is expected to be a string in the same format as SQL
        hints but without any comment characters. While you could use this to
        pass any SQL hint, the hints MONITOR (turn on monitoring) and
        NO_MONITOR (turn off monitoring) are the most useful. Use of this
        method requires Oracle Client 21.3 or higher (or Oracle Client 19 from
        19.11).

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if not isinstance(value, str):
            raise TypeError("expecting a string")
        self._hint = value
        return self

    def lock(self) -> "SodaOperation":
        """
        Specifies whether the documents fetched from the collection should be
        locked (equivalent to SQL "select for update"). Use of this method
        requires Oracle Client 21.3 or higher (or Oracle Client 19 from 19.11).

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        self._lock = True
        return self

    def key(self, value: str) -> "SodaOperation":
        """
        Specifies that the document with the specified key should be returned.
        This causes any previous calls made to this method and keys() to be
        ignored.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if not isinstance(value, str):
            raise TypeError("expecting string")
        self._key = value
        self._keys = None
        return self

    def keys(self, value: list) -> "SodaOperation":
        """
        Specifies that documents that match the keys found in the supplied
        sequence should be returned. This causes any previous calls made to
        this method and key() to be ignored.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        value_as_list = list(value)
        for element in value_as_list:
            if not isinstance(element, str):
                raise TypeError("expecting sequence of strings")
        self._keys = value_as_list
        self._key = None
        return self

    def limit(self, value: int) -> "SodaOperation":
        """
        Specifies that only the specified number of documents should be
        returned. This method is only usable for read operations such as
        getCursor() and getDocuments(). For write operations, any value set
        using this method is ignored.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if not isinstance(value, int) or value <= 0:
            raise TypeError("expecting positive integer")
        self._limit = value
        return self

    def remove(self) -> int:
        """
        Removes all of the documents in the collection that match the criteria.
        The number of documents that have been removed is returned.
        """
        return self._collection._impl.remove(self)

    def replaceOne(self, doc: Any) -> bool:
        """
        Replaces a single document in the collection with the specified
        document. The input document can be a dictionary or list or an existing
        SODA document object. A boolean indicating if a document was replaced
        or not is returned.

        Currently the method key() must be called before this method can be
        called.
        """
        doc_impl = self._collection._process_doc_arg(doc)
        return self._collection._impl.replace_one(
            self, doc_impl, return_doc=False
        )

    def replaceOneAndGet(self, doc: Any) -> "SodaDocument":
        """
        Similarly to replaceOne(), this method replaces a single document in
        the collection with the specified document. The only difference is that
        it returns a SodaDocument object. Note that for performance reasons the
        returned document does not contain the content.
        """
        doc_impl = self._collection._process_doc_arg(doc)
        return_doc_impl = self._collection._impl.replace_one(
            self, doc_impl, return_doc=True
        )
        return SodaDocument._from_impl(return_doc_impl)

    def skip(self, value: int) -> "SodaOperation":
        """
        Specifies the number of documents that match the other criteria that
        will be skipped. This method is only usable for read operations such as
        getCursor() and getDocuments(). For write operations, any value set
        using this method is ignored.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if not isinstance(value, int) or value < 0:
            raise TypeError("expecting integer >= 0")
        self._skip = value
        return self

    def version(self, value: str) -> "SodaOperation":
        """
        Specifies that documents with the specified version should be returned.
        Typically this is used with key() to implement optimistic locking, so
        that the write operation called later does not affect a document that
        someone else has modified.

        As a convenience, the SodaOperation object is returned so that further
        criteria can be specified by chaining methods together.
        """
        if not isinstance(value, str):
            raise TypeError("expecting string")
        self._version = value
        return self
