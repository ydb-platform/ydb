from functools import wraps
from typing import Union
from unittest.mock import Mock

from mongomock import DuplicateKeyError, helpers
from mongomock.collection import Collection
from mongomock.mongo_client import MongoClient

from .typing import DocumentType

try:
    from beanie.odm.fields import ExpressionField as _ExpressionField
except ModuleNotFoundError:
    ExpressionField = None
else:
    ExpressionField = _ExpressionField


def _provide_error_details(
    collection: Collection,
    data: DocumentType,
    exception: Exception,
) -> Union[Exception, DuplicateKeyError]:
    if not isinstance(exception, DuplicateKeyError):
        return exception

    for index in collection._store.indexes.values():
        if not index.get('unique'):
            continue

        is_sparse = index.get('sparse')

        find_kwargs = {}
        for key, _ in index.get('key'):
            try:
                find_kwargs[key] = helpers.get_value_by_dot(data, key)
            except KeyError:
                find_kwargs[key] = None

        if is_sparse and set(find_kwargs.values()) == {None}:
            continue

        found_documents = list(collection._iter_documents(find_kwargs))
        if len(found_documents) > 0:
            return DuplicateKeyError(
                'E11000 Duplicate Key Error',
                11000,
                {
                    'keyValue': find_kwargs,
                    'keyPattern': dict(index.get('key')),
                },
                None,
            )

    return exception


def _patch_insert_and_ensure_uniques(collection: Collection) -> Collection:
    """
    Adds details with 'keyPattern' and 'keyValue' when
    raising DuplicateKeyError from _insert or _ensure_uniques
    https://github.com/mongomock/mongomock/issues/773
    """

    def with_enriched_duplicate_key_error(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except DuplicateKeyError as exc:
                raise _provide_error_details(collection, args[0], exc)

        return wrapper

    collection._insert = with_enriched_duplicate_key_error(
        collection._insert,
    )
    collection._ensure_uniques = with_enriched_duplicate_key_error(
        collection._ensure_uniques,
    )

    return collection


def _normalize_strings(obj):
    if isinstance(obj, list):
        return [_normalize_strings(v) for v in obj]

    if isinstance(obj, tuple):
        return tuple(_normalize_strings(v) for v in obj)

    if isinstance(obj, dict):
        return {_normalize_strings(k): _normalize_strings(v) for k, v in obj.items()}

    # make sure we won't fail while working with beanie
    if ExpressionField and isinstance(obj, ExpressionField):
        return str(obj)

    return obj


def _patch_iter_documents_and_get_dataset(collection: Collection) -> Collection:
    """
    When using beanie or other solutions that utilize classes inheriting from
    the "str" type, we need to explicitly transform these instances to plain
    strings in cases where internal workings of "mongomock" unable to handle
    custom string-like classes. Currently only beanie's "ExpressionField" is
    transformed to plain strings.
    """

    def _iter_documents_with_normalized_strings(fn):
        @wraps(fn)
        def wrapper(filter):
            return fn(_normalize_strings(filter))

        return wrapper

    collection._iter_documents = _iter_documents_with_normalized_strings(
        collection._iter_documents,
    )

    def _get_dataset_with_normalized_strings(fn):
        @wraps(fn)
        def wrapper(spec, sort, fields, as_class):
            return fn(spec, _normalize_strings(sort), fields, as_class)

        return wrapper

    collection._get_dataset = _get_dataset_with_normalized_strings(
        collection._get_dataset,
    )

    return collection


def _patch_collection_internals(collection: Collection) -> Collection:
    if getattr(collection, '_patched_by_mongomock_motor', False):
        return collection
    collection = _patch_insert_and_ensure_uniques(collection)
    collection = _patch_iter_documents_and_get_dataset(collection)
    collection._patched_by_mongomock_motor = True  # type: ignore
    return collection


def _patch_client_internals(client: MongoClient) -> MongoClient:
    client.options = Mock(timeout=None)  # type: ignore
    return client


__all__ = ['_patch_collection_internals', '_patch_client_internals']
