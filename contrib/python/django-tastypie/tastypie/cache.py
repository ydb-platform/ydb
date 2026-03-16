from django.core.cache import caches


class NoCache(object):
    """
    A simplified, swappable base class for caching.

    Does nothing save for simulating the cache API.
    """
    def __init__(self, varies=None, *args, **kwargs):
        """
        Optionally accepts a ``varies`` list that will be used in the
        Vary header. Defaults to ["Accept"].
        """
        super(NoCache, self).__init__(*args, **kwargs)
        self.varies = varies

        if self.varies is None:
            self.varies = ["Accept"]

    def get(self, key):
        """
        Always returns ``None``.
        """
        return None

    def set(self, key, value, timeout=60):
        """
        No-op for setting values in the cache.
        """
        pass

    def cacheable(self, request, response):
        """
        Returns True or False if the request -> response is capable of being
        cached.
        """
        return bool(request.method == "GET" and response.status_code == 200)

    def cache_control(self):
        """
        No-op for returning values for cache-control
        """
        return {
            'no_cache': True,
        }


class SimpleCache(NoCache):
    """
    Uses Django's current ``CACHES`` configuration to store cached data.
    """

    def __init__(self, cache_name='default', timeout=None, public=None,
                 private=None, *args, **kwargs):
        """
        Optionally accepts a ``timeout`` in seconds for the resource's cache.
        Defaults to ``60`` seconds.
        """
        super(SimpleCache, self).__init__(*args, **kwargs)
        self.cache = caches[cache_name]
        self.timeout = timeout if timeout is not None else self.cache.default_timeout
        self.public = public
        self.private = private

    def get(self, key, **kwargs):
        """
        Gets a key from the cache. Returns ``None`` if the key is not found.
        """
        return self.cache.get(key, **kwargs)

    def set(self, key, value, timeout=None):
        """
        Sets a key-value in the cache.

        Optionally accepts a ``timeout`` in seconds. Defaults to ``None`` which
        uses the resource's default timeout.
        """

        if timeout is None:
            timeout = self.timeout

        self.cache.set(key, value, timeout)

    def cache_control(self):
        control = {
            'max_age': self.timeout,
            's_maxage': self.timeout,
        }

        if self.public is not None:
            control["public"] = self.public

        if self.private is not None:
            control["private"] = self.private

        return control
