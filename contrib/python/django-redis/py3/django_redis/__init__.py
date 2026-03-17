VERSION = (6, 0, 0)
__version__ = ".".join(map(str, VERSION))


def get_redis_connection(alias="default", write=True):
    """
    Helper used for obtaining a raw redis client.
    """

    from django.core.cache import caches

    cache = caches[alias]

    error_message = "This backend does not support this feature"
    if not hasattr(cache, "client"):
        raise NotImplementedError(error_message)

    if not hasattr(cache.client, "get_client"):
        raise NotImplementedError(error_message)

    return cache.client.get_client(write)
