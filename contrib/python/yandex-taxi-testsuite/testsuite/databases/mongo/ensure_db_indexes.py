import typing

import pymongo
import pytest

SORT_STR_TO_PYMONGO = {
    'ascending': pymongo.ASCENDING,
    'descending': pymongo.DESCENDING,
    '2d': pymongo.GEO2D,
    '2dsphere': pymongo.GEOSPHERE,
    'hashed': pymongo.HASHED,
    'text': pymongo.TEXT,
}


def create_collection(collection):
    try:
        collection.database.create_collection(collection.name)
    except pymongo.errors.CollectionInvalid:
        pass


def shard_collection(collection, sharding):
    db_admin = collection.database.client.admin
    try:
        db_admin.command('enablesharding', collection.database.name)
    except pymongo.errors.OperationFailure as exc:
        if exc.code != 23:
            raise
    kwargs = _get_kwargs_for_shard_func(sharding)
    if not _is_collection_sharded(collection):
        db_admin.command('shardcollection', collection.full_name, **kwargs)


def ensure_db_indexes(dbase, db_settings, sharding_enabled=True):
    for alias, value in db_settings.items():
        collection = getattr(dbase, alias, None)
        if collection is not None:
            create_collection(collection)

            indexes = value.get('indexes')
            if indexes:
                for index in indexes:
                    _ensure_index(index, collection)
                index_info = collection.index_information()
                assert len(index_info) == len(indexes) + 1, (
                    'Collection {} have {} indexes, but must have {} '.format(
                        alias,
                        len(index_info),
                        len(indexes) + 1,
                    )
                )

            if sharding_enabled:
                sharding = value.get('sharding')
                if sharding:
                    shard_collection(collection, sharding)


def _ensure_index(index, collection):
    arg, kwargs = _get_args_for_ensure_func(index)
    kwargs.pop('expireAfterSeconds', None)
    try:
        collection.create_index(arg, **kwargs)
    except pymongo.errors.OperationFailure as exc:
        pytest.fail(
            'ensure_index() failed for {}: {}'.format(collection.name, exc),
        )


def _get_args_for_ensure_func(index):
    kwargs = {}
    for key, value in index.items():
        if key == 'key':
            if isinstance(value, str):
                arg = index['key']
            elif isinstance(value, list):
                arg = []
                for obj in value:
                    arg.append((obj['name'], SORT_STR_TO_PYMONGO[obj['type']]))
        else:
            kwargs[key] = value

    if 'background' not in kwargs:
        kwargs['background'] = True

    return arg, kwargs


def _get_kwargs_for_shard_func(sharding):
    kwargs = {}

    for key, value in sharding.items():
        if key == 'key':
            sharding_key: dict[str, typing.Any]
            if isinstance(value, str):
                sharding_key = {value: 1}
            elif isinstance(value, list):
                sharding_key = {}
                for obj in value:
                    sharding_key[obj['name']] = SORT_STR_TO_PYMONGO[obj['type']]
            else:
                raise ValueError('Cannot handle key: {!r}'.format(value))
            kwargs['key'] = sharding_key
        else:
            kwargs[key] = value

    return kwargs


def _is_collection_sharded(collection):
    collstats = collection.database.command('collstats', collection.name)
    return collstats.get('sharded')
