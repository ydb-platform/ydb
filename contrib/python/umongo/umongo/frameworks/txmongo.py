from twisted.internet.defer import (
    inlineCallbacks, Deferred, DeferredList, returnValue, maybeDeferred)
from txmongo import filter as qf
from txmongo.database import Database
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


class TxMongoDocument(DocumentImplementation):

    __slots__ = ()

    opts = DocumentImplementation.opts

    @inlineCallbacks
    def reload(self):
        """
        Retrieve and replace document's data by the ones in database.

        Raises :class:`umongo.exceptions.NotCreatedError` if the document
        doesn't exist in database.
        """
        if not self.is_created:
            raise NotCreatedError("Document doesn't exists in database")
        ret = yield self.collection.find_one(self.pk)
        if ret is None:
            raise NotCreatedError("Document doesn't exists in database")
        self._data = self.DataProxy()
        self._data.from_mongo(ret)

    @inlineCallbacks
    def commit(self, io_validate_all=False, conditions=None, replace=False):
        """
        Commit the document in database.
        If the document doesn't already exist it will be inserted, otherwise
        it will be updated.

        :param io_validate_all:
        :param conditions: only perform commit if matching record in db
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
                    additional_filter = yield maybeDeferred(self.pre_update)
                    if additional_filter:
                        query.update(map_query(additional_filter, self.schema.fields))
                    self.required_validate()
                    yield self.io_validate(validate_all=io_validate_all)
                    if replace:
                        payload = self._data.to_mongo(update=False)
                        ret = yield self.collection.replace_one(query, payload)
                    else:
                        payload = self._data.to_mongo(update=True)
                        ret = yield self.collection.update_one(query, payload)
                    if ret.matched_count != 1:
                        raise UpdateError(ret)
                    yield maybeDeferred(self.post_update, ret)
                else:
                    ret = None
            elif conditions:
                raise NotCreatedError(
                    'Document must already exist in database to use `conditions`.'
                )
            else:
                yield maybeDeferred(self.pre_insert)
                self.required_validate()
                yield self.io_validate(validate_all=io_validate_all)
                payload = self._data.to_mongo(update=False)
                ret = yield self.collection.insert_one(payload)
                # TODO: check ret ?
                self._data.set(self.pk_field, ret.inserted_id)
                self.is_created = True
                yield maybeDeferred(self.post_insert, ret)
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

    @inlineCallbacks
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
        additional_filter = yield maybeDeferred(self.pre_delete)
        if additional_filter:
            query.update(map_query(additional_filter, self.schema.fields))
        ret = yield self.collection.delete_one(query)
        if ret.deleted_count != 1:
            raise DeleteError(ret)
        self.is_created = False
        yield maybeDeferred(self.post_delete, ret)
        return ret

    def io_validate(self, validate_all=False):
        """
        Run the io_validators of the document's fields.

        :param validate_all: If False only run the io_validators of the
            fields that have been modified.
        """
        if validate_all:
            return _io_validate_data_proxy(self.schema, self._data)
        return _io_validate_data_proxy(
            self.schema, self._data, partial=self._data.get_modified_fields())

    @classmethod
    @inlineCallbacks
    def find_one(cls, filter=None, *args, **kwargs):
        """
        Find a single document in database.
        """
        filter = cook_find_filter(cls, filter)
        ret = yield cls.collection.find_one(filter, *args, **kwargs)
        if ret is not None:
            ret = cls.build_from_mongo(ret, use_cls=True)
        return ret

    @classmethod
    @inlineCallbacks
    def find(cls, filter=None, *args, **kwargs):
        """
        Find a list document in database.

        Returns a list of Documents.
        """
        filter = cook_find_filter(cls, filter)
        raw_cursor_or_list = yield cls.collection.find(filter, *args, **kwargs)
        return [cls.build_from_mongo(e, use_cls=True) for e in raw_cursor_or_list]

    @classmethod
    @inlineCallbacks
    def find_with_cursor(cls, filter=None, *args, **kwargs):
        """
        Find a list document in database.

        Returns a cursor that provides Documents.
        """
        filter = cook_find_filter(cls, filter)
        raw_cursor_or_list = yield cls.collection.find_with_cursor(filter, *args, **kwargs)

        def wrap_raw_results(result):
            cursor = result[1]
            if cursor is not None:
                cursor.addCallback(wrap_raw_results)
            return ([cls.build_from_mongo(e, use_cls=True) for e in result[0]], cursor)

        return wrap_raw_results(raw_cursor_or_list)

    @classmethod
    def count(cls, filter=None, **kwargs):
        """
        Get the number of documents in this collection.
        """
        filter = cook_find_filter(cls, filter)
        return cls.collection.count(filter=filter, **kwargs)

    @classmethod
    @inlineCallbacks
    def ensure_indexes(cls):
        """
        Check&create if needed the Document's indexes in database
        """
        for index in cls.indexes:
            kwargs = index.document.copy()
            keys = kwargs.pop('key')
            index = qf.sort(keys.items())
            yield cls.collection.create_index(index, **kwargs)


def _errback_factory(errors, field=None, subkey=None):

    def errback(err):
        if isinstance(err.value, ma.ValidationError):
            error = err.value.messages
            if subkey is not None:
                error = {subkey: error}
            if field is not None:
                errors[field] = error
            else:
                errors.extend(error)
        else:
            raise err.value

    return errback


# Run multiple validators and collect all errors in one
@inlineCallbacks
def _run_validators(validators, field, value):
    errors = []
    defers = []
    for validator in validators:
        try:
            defer = validator(field, value)
        except ma.ValidationError as exc:
            errors.extend(exc.messages)
        else:
            if defer is None:
                continue
            assert isinstance(defer, Deferred), 'io_validate functions must return a Deferred'
            defer.addErrback(_errback_factory(errors))
            defers.append(defer)
    yield DeferredList(defers)
    if errors:
        raise ma.ValidationError(errors)


@inlineCallbacks
def _io_validate_data_proxy(schema, data_proxy, partial=None):
    errors = {}
    defers = []
    for name, field in schema.fields.items():
        if partial and name not in partial:
            continue
        value = data_proxy.get(name)
        if value is ma.missing:
            continue
        try:
            if field.io_validate_recursive:
                yield field.io_validate_recursive(field, value)
            if field.io_validate:
                defer = _run_validators(field.io_validate, field, value)
                defer.addErrback(_errback_factory(errors, name))
                defers.append(defer)
        except ma.ValidationError as exc:
            errors[name] = exc.messages
    yield DeferredList(defers)
    if errors:
        raise ma.ValidationError(errors)


@inlineCallbacks
def _reference_io_validate(field, value):
    if value is None:
        return
    yield value.fetch(no_data=True)


@inlineCallbacks
def _list_io_validate(field, value):
    if not value:
        return
    validators = field.inner.io_validate
    if not validators:
        return
    errors = {}
    defers = []
    for idx, exc in enumerate(value):
        defer = _run_validators(validators, field.inner, exc)
        defer.addErrback(_errback_factory(errors, idx))
        defers.append(defer)
    yield DeferredList(defers)
    if errors:
        raise ma.ValidationError(errors)


@inlineCallbacks
def _dict_io_validate(field, value):
    if not value or not field.value_field:
        return
    validators = field.value_field.io_validate
    if not validators:
        return
    errors = {}
    defers = []
    for key, exc in value.items():
        defer = _run_validators(validators, field.value_field, exc)
        defer.addErrback(_errback_factory(errors, key, subkey="value"))
        defers.append(defer)
    yield DeferredList(defers)
    if errors:
        raise ma.ValidationError(errors)


def _embedded_document_io_validate(field, value):
    if not value:
        return
    return _io_validate_data_proxy(value.schema, value._data)


class TxMongoReference(Reference):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._document = None

    @inlineCallbacks
    def fetch(self, no_data=False, force_reload=False):
        if not self._document or force_reload:
            if self.pk is None:
                raise NoneReferenceError('Cannot retrieve a None Reference')
            self._document = yield self.document_cls.find_one(self.pk)
            if not self._document:
                raise ma.ValidationError(self.error_messages['not_found'].format(
                    document=self.document_cls.__name__))
        returnValue(self._document)


class TxMongoBuilder(BaseBuilder):

    BASE_DOCUMENT_CLS = TxMongoDocument

    def _patch_field(self, field):
        super()._patch_field(field)

        validators = field.io_validate
        if not validators:
            field.io_validate = []
        else:
            if hasattr(validators, '__iter__'):
                validators = list(validators)
            else:
                validators = [validators]
            field.io_validate = validators
        if isinstance(field, ListField):
            field.io_validate_recursive = _list_io_validate
        if isinstance(field, DictField):
            field.io_validate_recursive = _dict_io_validate
        if isinstance(field, ReferenceField):
            field.io_validate.append(_reference_io_validate)
            field.reference_cls = TxMongoReference
        if isinstance(field, EmbeddedField):
            field.io_validate_recursive = _embedded_document_io_validate


class TxMongoInstance(Instance):
    """
    :class:`umongo.instance.Instance` implementation for txmongo
    """
    BUILDER_CLS = TxMongoBuilder

    @staticmethod
    def is_compatible_with(db):
        return isinstance(db, Database)


class TxMongoMigrationInstance(TxMongoInstance):
    """TxMongo instance with migration features"""

    @inlineCallbacks
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
            res = yield doc_cls.collection.find()
            for doc in res:
                doc = remove_cls_field_from_embedded_docs(doc, concrete_not_children)
                ret = yield doc_cls.collection.replace_one({"_id": doc["_id"]}, doc)
                if ret.matched_count != 1:
                    raise UpdateError(ret)
