import collections
from contextvars import ContextVar
from contextlib import contextmanager

from pymongo.database import Database
from pymongo.cursor import Cursor
from pymongo.errors import DuplicateKeyError
import marshmallow as ma

from ..builder import BaseBuilder
from ..instance import Instance
from ..document import DocumentImplementation
from ..data_objects import Reference
from ..exceptions import NotCreatedError, UpdateError, DeleteError, NoneReferenceError
from ..fields import ReferenceField, ListField, DictField, EmbeddedField
from ..query_mapper import map_query

from .tools import cook_find_filter, remove_cls_field_from_embedded_docs


SESSION = ContextVar("session", default=None)


# pymongo.Cursor defines __del__ method, hence mongomock's WrappedCursor should
# not inherit from this class otherwise garbage collection will crash...
class BaseWrappedCursor:

    __slots__ = ('raw_cursor', 'document_cls')

    def __init__(self, document_cls, cursor, *args, **kwargs):
        # Such a cunning plan my lord !
        # We inherit from Cursor but don't call its __init__ because
        # we act as a proxy to the underlying raw_cursor
        WrappedCursor.raw_cursor.__set__(self, cursor)
        WrappedCursor.document_cls.__set__(self, document_cls)

    def __getattr__(self, name):
        return getattr(self.raw_cursor, name)

    def __setattr__(self, name, value):
        return setattr(self.raw_cursor, name, value)

    def __getitem__(self, index):
        if isinstance(index, slice):
            elems = self.raw_cursor[index]
            return (self.document_cls.build_from_mongo(elem, use_cls=True)
                    for elem in elems)
        elem = self.raw_cursor[index]
        return self.document_cls.build_from_mongo(elem, use_cls=True)

    def __next__(self):
        elem = next(self.raw_cursor)
        return self.document_cls.build_from_mongo(elem, use_cls=True)

    def __iter__(self):
        for elem in self.raw_cursor:
            yield self.document_cls.build_from_mongo(elem, use_cls=True)


class WrappedCursor(BaseWrappedCursor, Cursor):
    __slots__ = ()


class PyMongoDocument(DocumentImplementation):

    __slots__ = ()
    cursor_cls = WrappedCursor  # Easier to customize this for mongomock this way

    opts = DocumentImplementation.opts

    def reload(self):
        """
        Retrieve and replace document's data by the ones in database.

        Raises :class:`umongo.exceptions.NotCreatedError` if the document
        doesn't exist in database.
        """
        if not self.is_created:
            raise NotCreatedError("Document doesn't exists in database")
        ret = self.collection.find_one(self.pk, session=SESSION.get())
        if ret is None:
            raise NotCreatedError("Document doesn't exists in database")
        self._data = self.DataProxy()
        self._data.from_mongo(ret)

    def commit(self, io_validate_all=False, conditions=None, replace=False):
        """
        Commit the document in database.
        If the document doesn't already exist it will be inserted, otherwise
        it will be updated.

        :param io_validate_all: Validate all field instead of only changed ones.
        :param conditions: Only perform commit if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`umongo.exceptions.UpdateError` if the
            conditions are not satisfied.
        :param replace: Replace the document rather than update.
        :return: A :class:`pymongo.results.UpdateResult` or
            :class:`pymongo.results.InsertOneResult` depending of the operation.
        """
        try:
            if self.is_created:
                if self.is_modified() or replace:
                    query = conditions or {}
                    query['_id'] = self.pk
                    # pre_update can provide additional query filter and/or
                    # modify the fields' values
                    additional_filter = self.pre_update()
                    if additional_filter:
                        query.update(map_query(additional_filter, self.schema.fields))
                    self.required_validate()
                    self.io_validate(validate_all=io_validate_all)
                    if replace:
                        payload = self._data.to_mongo(update=False)
                        ret = self.collection.replace_one(query, payload, session=SESSION.get())
                    else:
                        payload = self._data.to_mongo(update=True)
                        ret = self.collection.update_one(query, payload, session=SESSION.get())
                    if ret.matched_count != 1:
                        raise UpdateError(ret)
                    self.post_update(ret)
                else:
                    ret = None
            elif conditions:
                raise NotCreatedError(
                    'Document must already exist in database to use `conditions`.'
                )
            else:
                self.pre_insert()
                self.required_validate()
                self.io_validate(validate_all=io_validate_all)
                payload = self._data.to_mongo(update=False)
                ret = self.collection.insert_one(payload, session=SESSION.get())
                # TODO: check ret ?
                self._data.set(self.pk_field, ret.inserted_id)
                self.is_created = True
                self.post_insert(ret)
        except DuplicateKeyError as exc:
            # Sort value to make testing easier for compound indexes
            keys = sorted(exc.details['keyPattern'].keys())
            try:
                fields = [self.schema.fields[k] for k in keys]
            except KeyError:
                # A key in the index is unknwon from umongo
                raise exc
            if len(keys) == 1:
                msg = fields[0].error_messages['unique']
                raise ma.ValidationError({keys[0]: msg})
            raise ma.ValidationError({
                k: f.error_messages['unique_compound'].format(fields=keys)
                for k, f in zip(keys, fields)
            })
        self._data.clear_modified()
        return ret

    def delete(self, conditions=None):
        """
        Remove the document from database.

        :param conditions: Only perform delete if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`umongo.exceptions.DeleteError` if the
            conditions are not satisfied.
        Raises :class:`umongo.exceptions.NotCreatedError` if the document
        is not created (i.e. ``doc.is_created`` is False)
        Raises :class:`umongo.exceptions.DeleteError` if the document
        doesn't exist in database.

        :return: A :class:`pymongo.results.DeleteResult`
        """
        if not self.is_created:
            raise NotCreatedError("Document doesn't exists in database")
        query = conditions or {}
        query['_id'] = self.pk
        # pre_delete can provide additional query filter
        additional_filter = self.pre_delete()
        if additional_filter:
            query.update(map_query(additional_filter, self.schema.fields))
        ret = self.collection.delete_one(query, session=SESSION.get())
        if ret.deleted_count != 1:
            raise DeleteError(ret)
        self.is_created = False
        self.post_delete(ret)
        return ret

    def io_validate(self, validate_all=False):
        """
        Run the io_validators of the document's fields.

        :param validate_all: If False only run the io_validators of the
            fields that have been modified.
        """
        if validate_all:
            _io_validate_data_proxy(self.schema, self._data)
        else:
            _io_validate_data_proxy(
                self.schema, self._data, partial=self._data.get_modified_fields())

    @classmethod
    def find_one(cls, filter=None, *args, **kwargs):
        """
        Find a single document in database.
        """
        filter = cook_find_filter(cls, filter)
        ret = cls.collection.find_one(filter, session=SESSION.get(), *args, **kwargs)
        if ret is not None:
            ret = cls.build_from_mongo(ret, use_cls=True)
        return ret

    @classmethod
    def find(cls, filter=None, *args, **kwargs):
        """
        Find a list document in database.

        Returns a cursor that provide Documents.
        """
        filter = cook_find_filter(cls, filter)
        raw_cursor = cls.collection.find(filter, session=SESSION.get(), *args, **kwargs)
        return cls.cursor_cls(cls, raw_cursor)

    @classmethod
    def count_documents(cls, filter=None, **kwargs):
        """
        Get the number of documents in this collection.

        Unlike pymongo's collection.count_documents, filter is optional and
        defaults to an empty filter.
        """
        filter = cook_find_filter(cls, filter or {})
        return cls.collection.count_documents(filter, session=SESSION.get(), **kwargs)

    @classmethod
    def ensure_indexes(cls):
        """
        Check&create if needed the Document's indexes in database
        """
        if cls.indexes:
            cls.collection.create_indexes(cls.indexes, session=SESSION.get())


# Run multiple validators and collect all errors in one
def _run_validators(validators, field, value):
    if not hasattr(validators, '__iter__'):
        validators(field, value)
    else:
        errors = []
        for validator in validators:
            try:
                validator(field, value)
            except ma.ValidationError as exc:
                errors.extend(exc.messages)
        if errors:
            raise ma.ValidationError(errors)


def _io_validate_data_proxy(schema, data_proxy, partial=None):
    errors = {}
    for name, field in schema.fields.items():
        if partial and name not in partial:
            continue
        value = data_proxy.get(name)
        if value is ma.missing:
            continue
        try:
            if field.io_validate_recursive:
                field.io_validate_recursive(field, value)
            if field.io_validate:
                _run_validators(field.io_validate, field, value)
        except ma.ValidationError as exc:
            errors[name] = exc.messages
    if errors:
        raise ma.ValidationError(errors)


def _reference_io_validate(field, value):
    if value is None:
        return
    value.fetch(no_data=True)


def _list_io_validate(field, value):
    if not value:
        return
    errors = {}
    validators = field.inner.io_validate
    if not validators:
        return
    for idx, val in enumerate(value):
        try:
            _run_validators(validators, field.inner, val)
        except ma.ValidationError as exc:
            errors[idx] = exc.messages
    if errors:
        raise ma.ValidationError(errors)


def _dict_io_validate(field, value):
    if not value or not field.value_field:
        return
    errors = collections.defaultdict(dict)
    validators = field.value_field.io_validate
    if not validators:
        return
    for key, val in value.items():
        try:
            _run_validators(validators, field.value_field, val)
        except ma.ValidationError as exc:
            errors[key]["value"] = exc.messages
    if errors:
        raise ma.ValidationError(errors)


def _embedded_document_io_validate(field, value):
    if not value:
        return
    _io_validate_data_proxy(value.schema, value._data)


class PyMongoReference(Reference):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._document = None

    def fetch(self, no_data=False, force_reload=False):
        if not self._document or force_reload:
            if self.pk is None:
                raise NoneReferenceError('Cannot retrieve a None Reference')
            self._document = self.document_cls.find_one(self.pk)
            if not self._document:
                raise ma.ValidationError(self.error_messages['not_found'].format(
                    document=self.document_cls.__name__))
        return self._document


class PyMongoBuilder(BaseBuilder):

    BASE_DOCUMENT_CLS = PyMongoDocument

    def _patch_field(self, field):
        super()._patch_field(field)

        validators = field.io_validate
        if not validators:
            field.io_validate = []
        else:
            if hasattr(validators, '__iter__'):
                field.io_validate = list(validators)
            else:
                field.io_validate = [validators]
        if isinstance(field, ListField):
            field.io_validate_recursive = _list_io_validate
        if isinstance(field, DictField):
            field.io_validate_recursive = _dict_io_validate
        if isinstance(field, ReferenceField):
            field.io_validate.append(_reference_io_validate)
            field.reference_cls = PyMongoReference
        if isinstance(field, EmbeddedField):
            field.io_validate_recursive = _embedded_document_io_validate


class PyMongoInstance(Instance):
    """
    :class:`umongo.instance.Instance` implementation for pymongo
    """
    BUILDER_CLS = PyMongoBuilder

    @staticmethod
    def is_compatible_with(db):
        return isinstance(db, Database)

    @contextmanager
    def session(self):
        with self.db.client.start_session() as session:
            try:
                token = SESSION.set(session)
                yield session
            finally:
                SESSION.reset(token)


class PyMongoMigrationInstance(PyMongoInstance):
    """PyMongo instance with migration features"""

    def migrate_2_to_3(self):
        """Migrate database from umongo 2 to umongo 3

        - EmbeddedDocument _cls field is only set if child of concrete embedded document
        """
        concrete_not_children = [
            name for name, ed in self._embedded_lookup.items()
            if not ed.opts.is_child and not ed.opts.abstract
        ]

        for doc_cls in self._doc_lookup.values():
            if doc_cls.opts.abstract:
                continue
            if doc_cls.opts.is_child:
                continue
            for doc in doc_cls.collection.find():
                doc = remove_cls_field_from_embedded_docs(doc, concrete_not_children)
                ret = doc_cls.collection.replace_one({"_id": doc["_id"]}, doc)
                if ret.matched_count != 1:
                    raise UpdateError(ret)
