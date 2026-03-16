import datetime
import logging
import typing as _t

from cachelib.base import BaseCache
from cachelib.serializers import BaseSerializer


class MongoDbCache(BaseCache):
    """
    Implementation of cachelib.BaseCache that uses mongodb collection
    as the backend.

    Limitations: maximum MongoDB document size is 16mb

    :param client: mongodb client or connection string
    :param db: mongodb database name
    :param collection: mongodb collection name
    :param default_timeout: Set the timeout in seconds after which cache entries
                            expire
    :param key_prefix: A prefix that should be added to all keys.

    """

    serializer = BaseSerializer()

    def __init__(
        self,
        client: _t.Any = None,
        db: _t.Optional[str] = "cache-db",
        collection: _t.Optional[str] = "cache-collection",
        default_timeout: int = 300,
        key_prefix: _t.Optional[str] = None,
        **kwargs: _t.Any
    ):
        super().__init__(default_timeout)
        try:
            import pymongo  # type: ignore
        except ImportError:
            logging.warning("no pymongo module found")

        if client is None or isinstance(client, str):
            client = pymongo.MongoClient(host=client)
        self.client = client[db][collection]
        index_info = self.client.index_information()
        all_keys = {
            subkey[0] for value in index_info.values() for subkey in value["key"]
        }
        if "id" not in all_keys:
            self.client.create_index("id", unique=True)
        self.key_prefix = key_prefix or ""
        self.collection = collection

    def _utcnow(self) -> _t.Any:
        """Return a tz-aware UTC datetime representing the current time"""
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    def _expire_records(self) -> _t.Any:
        res = self.client.delete_many({"expiration": {"$lte": self._utcnow()}})
        return res

    def get(self, key: str) -> _t.Any:
        """
        Get a cache item

        :param key: The cache key of the item to fetch
        :return: cache value if not expired, else None
        """
        self._expire_records()
        record = self.client.find_one({"id": self.key_prefix + key})
        value = None
        if record:
            value = self.serializer.loads(record["val"])
        return value

    def delete(self, key: str) -> bool:
        """
        Deletes an item from the cache.  This is a no-op if the item doesn't
        exist

        :param key: Key of the item to delete.
        :return: True if the key existed and was deleted
        """
        res = self.client.delete_one({"id": self.key_prefix + key})
        deleted = bool(res.deleted_count > 0)
        return deleted

    def _set(
        self,
        key: str,
        value: _t.Any,
        timeout: _t.Optional[int] = None,
        overwrite: _t.Optional[bool] = True,
    ) -> _t.Any:
        """
        Store a cache item, with the option to not overwrite existing items

        :param key: Cache key to use
        :param value: a serializable object
        :param timeout: The timeout in seconds for the cached item, to override
                        the default
        :param overwrite: If true, overwrite any existing cache item with key.
                          If false, the new value will only be stored if no
                          non-expired cache item exists with key.
        :return: True if the new item was stored.
        """
        timeout = self._normalize_timeout(timeout)
        now = self._utcnow()

        if not overwrite:
            # fail if a non-expired item with this key
            # already exists
            if self.has(key):
                return False

        dump = self.serializer.dumps(value)
        record = {"id": self.key_prefix + key, "val": dump}

        if timeout > 0:
            record["expiration"] = now + datetime.timedelta(seconds=timeout)
        self.client.update_one({"id": self.key_prefix + key}, {"$set": record}, True)
        return True

    def set(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> _t.Any:
        self._expire_records()
        return self._set(key, value, timeout=timeout, overwrite=True)

    def set_many(
        self, mapping: _t.Dict[str, _t.Any], timeout: _t.Optional[int] = None
    ) -> _t.List[_t.Any]:
        self._expire_records()
        from pymongo import UpdateOne

        operations = []
        now = self._utcnow()
        timeout = self._normalize_timeout(timeout)
        for key, val in mapping.items():
            dump = self.serializer.dumps(val)

            record = {"id": self.key_prefix + key, "val": dump}

            if timeout > 0:
                record["expiration"] = now + datetime.timedelta(seconds=timeout)
            operations.append(
                UpdateOne({"id": self.key_prefix + key}, {"$set": record}, upsert=True),
            )

        result = self.client.bulk_write(operations)
        keys = list(mapping.keys())
        if result.bulk_api_result["nUpserted"] != len(keys):
            query = self.client.find(
                {"id": {"$in": [self.key_prefix + key for key in keys]}}
            )
            keys = []
            for item in query:
                keys.append(item["id"])
        return keys

    def get_many(self, *keys: str) -> _t.List[_t.Any]:
        results = self.get_dict(*keys)
        values = []
        for key in keys:
            values.append(results.get(key, None))
        return values

    def get_dict(self, *keys: str) -> _t.Dict[str, _t.Any]:
        self._expire_records()
        query = self.client.find(
            {"id": {"$in": [self.key_prefix + key for key in keys]}}
        )
        results = dict.fromkeys(keys, None)
        for item in query:
            value = self.serializer.loads(item["val"])
            results[item["id"][len(self.key_prefix) :]] = value
        return results

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> _t.Any:
        self._expire_records()
        return self._set(key, value, timeout=timeout, overwrite=False)

    def has(self, key: str) -> bool:
        self._expire_records()
        record = self.get(key)
        return record is not None

    def delete_many(self, *keys: str) -> _t.List[_t.Any]:
        self._expire_records()
        res = list(keys)
        filter = {"id": {"$in": [self.key_prefix + key for key in keys]}}
        result = self.client.delete_many(filter)

        if result.deleted_count != len(keys):
            existing_keys = [
                item["id"][len(self.key_prefix) :] for item in self.client.find(filter)
            ]
            res = [item for item in keys if item not in existing_keys]

        return res

    def clear(self) -> bool:
        self.client.drop()
        return True
