"""
This module contains both functions of setting and getting from redis
Redis is used for storing the pickled query profiled data, and later to retrieve it back
"""
import pickle
import uuid

import redis
from django.conf import settings

from django_query_profiler.query_profiler_storage import QueryProfiledData

REDIS_INSTANCE = redis.StrictRedis(
    host=settings.DJANGO_QUERY_PROFILER_REDIS_HOST,
    port=settings.DJANGO_QUERY_PROFILER_REDIS_PORT,
    db=settings.DJANGO_QUERY_PROFILER_REDIS_DB)


def store_data(query_profiled_data: QueryProfiledData) -> str:
    pickled_query_profiled_data = pickle.dumps(query_profiled_data)
    redis_key = str(uuid.uuid4().hex)
    ttl_seconds: int = settings.DJANGO_QUERY_PROFILER_REDIS_KEYS_EXPIRY_SECONDS
    REDIS_INSTANCE.set(name=_get_key(redis_key), value=pickled_query_profiled_data, ex=ttl_seconds)
    return redis_key


def retrieve_data(redis_key: str) -> QueryProfiledData:
    redis_object = REDIS_INSTANCE.get(_get_key(redis_key))
    return pickle.loads(redis_object)


def _get_key(redis_key: str) -> str:
    """ Trying to create a namespace for django_query_profiler keys.  We could have used redis key-space as well """
    redis_key_prefix = 'django_query_profiler_'
    return f'{redis_key_prefix}:{redis_key}'
