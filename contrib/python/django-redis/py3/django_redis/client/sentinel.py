from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from django.core.exceptions import ImproperlyConfigured
from redis.sentinel import SentinelConnectionPool

from django_redis.client.default import DefaultClient


def replace_query(url, query):
    return urlunparse((*url[:4], urlencode(query, doseq=True), url[5]))


class SentinelClient(DefaultClient):
    """
    Sentinel client which uses the single redis URL specified by the CACHE's
    LOCATION to create a LOCATION configuration for two connection pools; One
    pool for the primaries and another pool for the replicas, and upon
    connecting ensures the connection pool factory is configured correctly.
    """

    def __init__(self, server, params, backend):
        if isinstance(server, str):
            url = urlparse(server)
            primary_query = parse_qs(url.query, keep_blank_values=True)
            replica_query = dict(primary_query)
            primary_query["is_master"] = [1]
            replica_query["is_master"] = [0]

            server = [replace_query(url, i) for i in (primary_query, replica_query)]

        super().__init__(server, params, backend)

    def connect(self, *args, **kwargs):
        connection = super().connect(*args, **kwargs)
        if not isinstance(connection.connection_pool, SentinelConnectionPool):
            error_message = (
                "Settings DJANGO_REDIS_CONNECTION_FACTORY or "
                "CACHE[].OPTIONS.CONNECTION_POOL_CLASS is not configured correctly."
            )
            raise ImproperlyConfigured(error_message)

        return connection
