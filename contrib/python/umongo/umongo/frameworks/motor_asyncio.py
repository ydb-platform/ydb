import collections
from contextvars import ContextVar
from contextlib import asynccontextmanager

from inspect import iscoroutine
import asyncio

from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCursor
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


class WrappedCursor(AsyncIOMotorCursor):

    __slots__ = ('raw_cursor', 'document_cls')

    def __init__(self, document_cls, cursor):
        # Such a cunning plan my lord !
        # We inherit from Cursor but don't call its __init__ because
        # we act as a proxy to the underlying raw_cursor
        WrappedCursor.raw_cursor.__set__(self, cursor)
        WrappedCursor.document_cls.__set__(self, document_cls)

    def __getattr__(self, name):
        return getattr(self.raw_cursor, name)

    def __setattr__(self, name, value):
        return setattr(self.raw_cursor, name, value)

    def clone(self):
        return WrappedCursor(self.document_cls, self.raw_cursor.clone())

    async def next(self):
        raw = await self.raw_cursor.__anext__()
        return self.document_cls.build_from_mongo(raw, use_cls=True)

    __anext__ = next

    def next_object(self):
        raw = self.raw_cursor.next_object()
        return self.document_cls.build_from_mongo(raw, use_cls=True)

    def each(self, callback):
        def wrapped_callback(result, error):
            if not error and result is not None:
                result = self.document_cls.build_from_mongo(result, use_cls=True)
            return callback(result, error)
        return self.raw_cursor.each(wrapped_callback)

    def to_list(self, length, callback=None):
        kwargs = {"callback": callback} if callback else {}
        raw_future = self.raw_cursor.to_list(length, **kwargs)
        cooked_future = asyncio.Future()
        builder = self.document_cls.build_from_mongo

        def on_raw_done(fut):
            cooked_future.set_result([builder(e, use_cls=True) for e in fut.result()])

        raw_future.add_done_callback(on_raw_done)
        return cooked_future


class MotorAsyncIODocument(DocumentImplementation):

    __slots__ = ()

    opts = DocumentImplementation.opts

    # Cook hooks into coroutines in order to allow them to return
    # either Future or regular return value.

    async def __coroutined_pre_insert(self):
        ret = self.pre_insert()
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def __coroutined_pre_update(self):
        ret = self.pre_update()
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def __coroutined_pre_delete(self):
        ret = self.pre_delete()
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def __coroutined_post_insert(self, ret):
        ret = self.post_insert(ret)
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def __coroutined_post_update(self, ret):
        ret = self.post_update(ret)
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def __coroutined_post_delete(self, ret):
        ret = self.post_delete(ret)
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def reload(self):
        """
        Retrieve and replace document's data by the ones in database.

        Raises :class:`umongo.exceptions.NotCreatedError` if the document
        doesn't exist in database.
        """
        if not self.is_created:
            raise NotCreatedError("Document doesn't exists in database")
        ret = await self.collection.find_one(self.pk, session=SESSION.get())
        if ret is None:
            raise NotCreatedError("Document doesn't exists in database")
        self._data = self.DataProxy()
        self._data.from_mongo(ret)

    async def commit(self, io_validate_all=False, conditions=None, replace=False):
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
        :return: Update result dict returned by underlaying driver or
            ObjectId of the inserted document.
        """
        try:
            if self.is_created:
                if self.is_modified() or replace:
                    query = conditions or {}
                    query['_id'] = self.pk
                    # pre_update can provide additional query filter and/or
                    # modify the fields' values
                    additional_filter = await self.__coroutined_pre_update()
                    if additional_filter:
                        query.update(map_query(additional_filter, self.schema.fields))
                    self.required_validate()
                    await self.io_validate(validate_all=io_validate_all)
                    if replace:
                        payload = self._data.to_mongo(update=False)
                        ret = await self.collection.replace_one(
                            query, payload, session=SESSION.get())
                    else:
                        payload = self._data.to_mongo(update=True)
                        ret = await self.collection.update_one(
                            query, payload, session=SESSION.get())
                    if ret.matched_count != 1:
                        raise UpdateError(ret)
                    await self.__coroutined_post_update(ret)
                else:
                    ret = None
            elif conditions:
                raise NotCreatedError(
                    'Document must already exist in database to use `conditions`.'
                )
            else:
                await self.__coroutined_pre_insert()
                self.required_validate()
                await self.io_validate(validate_all=io_validate_all)
                payload = self._data.to_mongo(update=False)
                ret = await self.collection.insert_one(payload, session=SESSION.get())
                # TODO: check ret ?
                self._data.set(self.pk_field, ret.inserted_id)
                self.is_created = True
                await self.__coroutined_post_insert(ret)
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

    async def delete(self, conditions=None):
        """
        Alias of :meth:`remove` to enforce default api.
        """
        return await self.remove(conditions=conditions)

    async def remove(self, conditions=None):
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

        :return: Delete result dict returned by underlaying driver.
        """
        if not self.is_created:
            raise NotCreatedError("Document doesn't exists in database")
        query = conditions or {}
        query['_id'] = self.pk
        # pre_delete can provide additional query filter
        additional_filter = await self.__coroutined_pre_delete()
        if additional_filter:
            query.update(map_query(additional_filter, self.schema.fields))
        ret = await self.collection.delete_one(query, session=SESSION.get())
        if ret.deleted_count != 1:
            raise DeleteError(ret)
        self.is_created = False
        await self.__coroutined_post_delete(ret)
        return ret

    async def io_validate(self, validate_all=False):
        """
        Run the io_validators of the document's fields.

        :param validate_all: If False only run the io_validators of the
            fields that have been modified.
        """
        if validate_all:
            return await _io_validate_data_proxy(self.schema, self._data)
        return await _io_validate_data_proxy(
            self.schema, self._data, partial=self._data.get_modified_fields())

    @classmethod
    async def find_one(cls, filter=None, *args, **kwargs):
        """
        Find a single document in database.
        """
        filter = cook_find_filter(cls, filter)
        ret = await cls.collection.find_one(filter, session=SESSION.get(), *args, **kwargs)
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
        return WrappedCursor(
            cls,
            cls.collection.find(filter, session=SESSION.get(), *args, **kwargs)
        )

    @classmethod
    async def count_documents(cls, filter=None, *, with_limit_and_skip=False, **kwargs):
        """
        Return a count of the documents in a collection.
        """
        filter = cook_find_filter(cls, filter or {})
        return await cls.collection.count_documents(filter, session=SESSION.get(), **kwargs)

    @classmethod
    async def ensure_indexes(cls):
        """
        Check&create if needed the Document's indexes in database
        """
        for index in cls.indexes:
            kwargs = index.document.copy()
            keys = kwargs.pop('key').items()
            await cls.collection.create_index(keys, session=SESSION.get(), **kwargs)


# Run multiple validators and collect all errors in one
async def _run_validators(validators, field, value):
    errors = []
    tasks = [validator(field, value) for validator in validators]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        if isinstance(res, ma.ValidationError):
            errors.extend(res.messages)
        elif res:
            raise res
    if errors:
        raise ma.ValidationError(errors)


async def _io_validate_data_proxy(schema, data_proxy, partial=None):
    errors = {}
    tasks = []
    tasks_field_name = []
    for name, field in schema.fields.items():
        if partial and name not in partial:
            continue
        value = data_proxy.get(name)
        if value is ma.missing:
            continue
        try:
            if field.io_validate_recursive:
                await field.io_validate_recursive(field, value)
            if field.io_validate:
                tasks.append(_run_validators(field.io_validate, field, value))
                tasks_field_name.append(name)
        except ma.ValidationError as exc:
            errors[name] = exc.messages
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, res in enumerate(results):
        if isinstance(res, ma.ValidationError):
            errors[tasks_field_name[i]] = res.messages
        elif res:
            raise res
    if errors:
        raise ma.ValidationError(errors)


async def _reference_io_validate(field, value):
    if value is None:
        return
    await value.fetch(no_data=True)


async def _list_io_validate(field, value):
    if not value:
        return
    validators = field.inner.io_validate
    if not validators:
        return
    tasks = [_run_validators(validators, field.inner, e) for e in value]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = {}
    for i, res in enumerate(results):
        if isinstance(res, ma.ValidationError):
            errors[i] = res.messages
        elif res:
            raise res
    if errors:
        raise ma.ValidationError(errors)


async def _dict_io_validate(field, value):
    if not value or not field.value_field:
        return
    validators = field.value_field.io_validate
    if not validators:
        return
    tasks = []
    for key, val in value.items():
        tasks.append(_run_validators(validators, field.value_field, val))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = collections.defaultdict(dict)
    for key, res in zip(value.keys(), results):
        if isinstance(res, ma.ValidationError):
            errors[key]["value"] = res.messages
        elif res:
            raise res
    if errors:
        raise ma.ValidationError(errors)


async def _embedded_document_io_validate(field, value):
    if not value:
        return
    await _io_validate_data_proxy(value.schema, value._data)


class MotorAsyncIOReference(Reference):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._document = None

    async def fetch(self, no_data=False, force_reload=False):
        if not self._document or force_reload:
            if self.pk is None:
                raise NoneReferenceError('Cannot retrieve a None Reference')
            self._document = await self.document_cls.find_one(self.pk)
            if not self._document:
                raise ma.ValidationError(self.error_messages['not_found'].format(
                    document=self.document_cls.__name__))
        return self._document


class MotorAsyncIOBuilder(BaseBuilder):

    BASE_DOCUMENT_CLS = MotorAsyncIODocument

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
            field.io_validate = [
                v if asyncio.iscoroutinefunction(v) else asyncio.coroutine(v)
                for v in validators
            ]
        if isinstance(field, ListField):
            field.io_validate_recursive = _list_io_validate
        if isinstance(field, DictField):
            field.io_validate_recursive = _dict_io_validate
        if isinstance(field, ReferenceField):
            field.io_validate.append(_reference_io_validate)
            field.reference_cls = MotorAsyncIOReference
        if isinstance(field, EmbeddedField):
            field.io_validate_recursive = _embedded_document_io_validate


class MotorAsyncIOInstance(Instance):
    """
    :class:`umongo.instance.Instance` implementation for motor-asyncio
    """
    BUILDER_CLS = MotorAsyncIOBuilder

    @staticmethod
    def is_compatible_with(db):
        return isinstance(db, AsyncIOMotorDatabase)

    @asynccontextmanager
    async def session(self):
        async with await self.db.client.start_session() as session:
            try:
                token = SESSION.set(session)
                yield session
            finally:
                SESSION.reset(token)


class MotorAsyncIOMigrationInstance(MotorAsyncIOInstance):
    """AsyncIO instance with migration features"""

    async def migrate_2_to_3(self):
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
            async for doc in doc_cls.collection.find():
                doc = remove_cls_field_from_embedded_docs(doc, concrete_not_children)
                ret = await doc_cls.collection.replace_one({"_id": doc["_id"]}, doc)
                if ret.matched_count != 1:
                    raise UpdateError(ret)
