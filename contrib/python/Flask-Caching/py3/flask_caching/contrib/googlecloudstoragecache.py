import datetime
import json
import logging

from flask_caching.backends.base import BaseCache


logger = logging.getLogger(__name__)


try:
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage, exceptions
except ImportError as e:
    raise RuntimeError("no google-cloud-storage module found") from e


class GoogleCloudStorageCache(BaseCache):
    """Uses an Google Cloud Storage bucket as a cache backend.
    Note: User-contributed functionality. This project does not guarantee that
    this functionality will be maintained or functional at any given time.
    Note: Cache keys must meet GCS criteria for a valid object name (a sequence
    of Unicode characters whose UTF-8 encoding is at most 1024 bytes long).
    Note: Expired cache objects are not automatically purged. If
    delete_expired_objects_on_read=True, they will be deleted following an
    attempted read (which reduces performance). Otherwise, you have to delete
    stale objects yourself. Consider an GCS bucket lifecycle rule or other
    out-of-band process. For example you can use the following rule.
    {"rule": [{"action": {"type": "Delete"}, "condition": {"daysSinceCustomTime": 0}}]}
    https://cloud.google.com/storage/docs/lifecycle#dayssincecustomtime
    :param bucket: Required. Name of the bucket to use. It must already exist.
    :param key_prefix: A prefix that should be added to all keys.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param delete_expired_objects_on_read: If True, if a read finds a stale
                                           object, it will be deleted before
                                           a response is returned. Will slow
                                           down responses.
    :param anonymous: If true, use anonymous credentials. Useful for testing.
    Any additional keyword arguments will be passed to ``google.cloud.storage.Client``.
    """

    def __init__(
        self,
        bucket,
        key_prefix=None,
        default_timeout=300,
        delete_expired_objects_on_read=False,
        anonymous=False,
        **kwargs
    ):
        super().__init__(default_timeout)
        if not isinstance(bucket, str):
            raise ValueError("GCSCache bucket parameter must be a string")
        if anonymous:
            self._client = storage.Client(
                credentials=AnonymousCredentials(), project="test", **kwargs
            )
        else:
            self._client = storage.Client(**kwargs)
        self.bucket = self._client.get_bucket(bucket)
        self.key_prefix = key_prefix or ""
        self.default_timeout = default_timeout
        self.delete_expired_objects_on_read = delete_expired_objects_on_read

    @classmethod
    def factory(cls, app, config, args, kwargs):
        args.insert(0, config["CACHE_GCS_BUCKET"])
        key_prefix = config.get("CACHE_KEY_PREFIX")
        if key_prefix:
            kwargs["key_prefix"] = key_prefix
        return cls(*args, **kwargs)

    def get(self, key):
        result = None
        expired = False
        hit_or_miss = "miss"
        full_key = self.key_prefix + key
        blob = self.bucket.get_blob(full_key)
        if blob is not None:
            expired = blob.custom_time and self._now() > blob.custom_time
            if expired:
                # Object is stale
                if self.delete_expired_objects_on_read:
                    self._delete(full_key)
            else:
                try:
                    result = blob.download_as_bytes()
                    hit_or_miss = "hit"
                    if blob.content_type == "application/json":
                        result = json.loads(result)
                except exceptions.NotFound:
                    pass
        expiredstr = "(expired)" if expired else ""
        logger.debug("get key %r -> %s %s", full_key, hit_or_miss, expiredstr)
        return result

    def set(self, key, value, timeout=None):
        result = False
        full_key = self.key_prefix + key
        content_type = "application/json"
        try:
            value = json.dumps(value)
        except (UnicodeDecodeError, TypeError):
            content_type = "application/octet-stream"
        blob = self.bucket.blob(full_key)
        if timeout is None:
            timeout = self.default_timeout
        if timeout != 0:
            # Use 'Custom-Time' for expiry
            # https://cloud.google.com/storage/docs/metadata#custom-time
            blob.custom_time = self._now(delta=timeout)
        try:
            blob.upload_from_string(value, content_type=content_type)
            result = True
        except exceptions.TooManyRequests:
            pass
        logger.debug("set key %r -> %s", full_key, result)
        return result

    def add(self, key, value, timeout=None):
        full_key = self.key_prefix + key
        if self._has(full_key):
            logger.debug("add key %r -> not added", full_key)
            return False
        else:
            return self.set(key, value, timeout)

    def delete(self, key):
        full_key = self.key_prefix + key
        return self._delete(full_key)

    def delete_many(self, *keys):
        return self._delete_many(self.key_prefix + key for key in keys)

    def has(self, key):
        full_key = self.key_prefix + key
        return self._has(full_key)

    def clear(self):
        return self._prune(clear_all=True)

    def _prune(self, clear_all=False):
        # Delete in batches of 100 which is much faster than individual deletes
        nremoved = 0
        now = self._now()
        response_iterator = self._client.list_blobs(
            self.bucket,
            prefix=self.key_prefix,
            fields="items(name,customTime),nextPageToken",
        )
        to_delete = []
        for blob in response_iterator:
            if clear_all or blob.custom_time and blob.custom_time < now:
                to_delete.append(blob.name)
                nremoved += 1
                if len(to_delete) == 100:
                    self._delete_many(to_delete)
                    to_delete = []
        # Delete the remainder
        if to_delete:
            self._delete_many(to_delete)
        logger.debug("evicted %d key(s)", nremoved)
        return True

    def _delete(self, key):
        return self._delete_many([key])

    def _delete_many(self, keys):
        try:
            with self._client.batch():
                for key in keys:
                    self.bucket.delete_blob(key)
        except (exceptions.NotFound, exceptions.TooManyRequests):
            pass
        return True

    def _has(self, key):
        result = False
        expired = False
        blob = self.bucket.get_blob(key)
        if blob is not None:
            expired = blob.custom_time and self._now() > blob.custom_time
            if expired:
                # Exists but is stale
                if self.delete_expired_objects_on_read:
                    self._delete(key)
            else:
                result = True
        expiredstr = "(expired)" if expired else ""
        logger.debug("has key %r -> %s %s", key, result, expiredstr)
        return result

    def _now(self, delta=0):
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=delta
        )
