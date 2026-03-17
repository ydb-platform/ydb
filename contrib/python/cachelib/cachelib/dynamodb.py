import datetime
import typing as _t

from cachelib.base import BaseCache
from cachelib.serializers import DynamoDbSerializer

CREATED_AT_FIELD = "created_at"
RESPONSE_FIELD = "response"


class DynamoDbCache(BaseCache):
    """
    Implementation of cachelib.BaseCache that uses an AWS DynamoDb table
    as the backend.

    Your server process will require dynamodb:GetItem and dynamodb:PutItem
    IAM permissions on the cache table.

    Limitations: DynamoDB table items are limited to 400 KB in size.  Since
    this class stores cached items in a table, the max size of a cache entry
    will be slightly less than 400 KB, since the cache key and expiration
    time fields are also part of the item.

    :param table_name: The name of the DynamoDB table to use
    :param default_timeout: Set the timeout in seconds after which cache entries
                            expire
    :param key_field: The name of the hash_key attribute in the DynamoDb
                      table. This must be a string attribute.
    :param expiration_time_field: The name of the table attribute to store the
                                  expiration time in.  This will be an int
                                  attribute. The timestamp will be stored as
                                  seconds past the epoch.  If you configure
                                  this as the TTL field, then DynamoDB will
                                  automatically delete expired entries.
    :param key_prefix: A prefix that should be added to all keys.

    """

    serializer = DynamoDbSerializer()

    def __init__(
        self,
        table_name: _t.Optional[str] = "python-cache",
        default_timeout: int = 300,
        key_field: _t.Optional[str] = "cache_key",
        expiration_time_field: _t.Optional[str] = "expiration_time",
        key_prefix: _t.Optional[str] = None,
        **kwargs: _t.Any
    ):
        super().__init__(default_timeout)

        try:
            import boto3  # type: ignore
        except ImportError as err:
            raise RuntimeError("no boto3 module found") from err

        self._table_name = table_name
        self._key_field = key_field
        self._expiration_time_field = expiration_time_field
        self.key_prefix = key_prefix or ""
        self._dynamo = boto3.resource("dynamodb", **kwargs)
        self._attr = boto3.dynamodb.conditions.Attr

        try:
            self._table = self._dynamo.Table(table_name)
            self._table.load()
            # catch this exception (triggered if the table doesn't exist)
        except Exception:
            table = self._dynamo.create_table(
                AttributeDefinitions=[
                    {"AttributeName": key_field, "AttributeType": "S"}
                ],
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": key_field, "KeyType": "HASH"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            table.wait_until_exists()
            dynamo = boto3.client("dynamodb", **kwargs)
            dynamo.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": expiration_time_field,
                },
            )
            self._table = self._dynamo.Table(table_name)
            self._table.load()

    def _utcnow(self) -> _t.Any:
        """Return a tz-aware UTC datetime representing the current time"""
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    def _get_item(self, key: str, attributes: _t.Optional[list] = None) -> _t.Any:
        """
        Get an item from the cache table, optionally limiting the returned
        attributes.

        :param key: The cache key of the item to fetch

        :param attributes: An optional list of attributes to fetch.  If not
                           given, all attributes are fetched.  The
                           expiration_time field will always be added to the
                           list of fetched attributes.
        :return: The table item for key if it exists and is not expired, else
                 None
        """
        kwargs = {}
        if attributes:
            if self._expiration_time_field not in attributes:
                attributes = list(attributes) + [self._expiration_time_field]
            kwargs = dict(ProjectionExpression=",".join(attributes))

        response = self._table.get_item(Key={self._key_field: key}, **kwargs)
        cache_item = response.get("Item")

        if cache_item:
            now = int(self._utcnow().timestamp())
            if cache_item.get(self._expiration_time_field, now + 100) > now:
                return cache_item

        return None

    def get(self, key: str) -> _t.Any:
        """
        Get a cache item

        :param key: The cache key of the item to fetch
        :return: cache value if not expired, else None
        """
        cache_item = self._get_item(self.key_prefix + key)
        if cache_item:
            response = cache_item[RESPONSE_FIELD]
            value = self.serializer.loads(response)
            return value
        return None

    def delete(self, key: str) -> bool:
        """
        Deletes an item from the cache.  This is a no-op if the item doesn't
        exist

        :param key: Key of the item to delete.
        :return: True if the key existed and was deleted
        """
        try:
            self._table.delete_item(
                Key={self._key_field: self.key_prefix + key},
                ConditionExpression=self._attr(self._key_field).exists(),
            )
            return True
        except self._dynamo.meta.client.exceptions.ConditionalCheckFailedException:
            return False

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

        kwargs = {}
        if not overwrite:
            # Cause the put to fail if a non-expired item with this key
            # already exists

            cond = self._attr(self._key_field).not_exists() | self._attr(
                self._expiration_time_field
            ).lte(int(now.timestamp()))
            kwargs = dict(ConditionExpression=cond)

        try:
            dump = self.serializer.dumps(value)
            item = {
                self._key_field: key,
                CREATED_AT_FIELD: now.isoformat(),
                RESPONSE_FIELD: dump,
            }
            if timeout > 0:
                expiration_time = now + datetime.timedelta(seconds=timeout)
                item[self._expiration_time_field] = int(expiration_time.timestamp())
            self._table.put_item(Item=item, **kwargs)
            return True
        except Exception:
            return False

    def set(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> _t.Any:
        return self._set(self.key_prefix + key, value, timeout=timeout, overwrite=True)

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> _t.Any:
        return self._set(self.key_prefix + key, value, timeout=timeout, overwrite=False)

    def has(self, key: str) -> bool:
        return (
            self._get_item(self.key_prefix + key, [self._expiration_time_field])
            is not None
        )

    def clear(self) -> bool:
        paginator = self._dynamo.meta.client.get_paginator("scan")
        pages = paginator.paginate(
            TableName=self._table_name, ProjectionExpression=self._key_field
        )

        with self._table.batch_writer() as batch:
            for page in pages:
                for item in page["Items"]:
                    batch.delete_item(Key=item)

        return True
