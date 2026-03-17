==========================
Redis Django Cache Backend
==========================

.. image:: https://pepy.tech/badge/django-redis-cache
    :target: https://pepy.tech/project/django-redis-cache
    :alt: Downloads

.. image:: https://img.shields.io/pypi/v/django-redis-cache.svg
    :target: https://pypi.python.org/pypi/django-redis-cache/
    :alt: Latest Version

.. image:: https://img.shields.io/travis/sebleier/django-redis-cache.svg
    :target: https://travis-ci.org/sebleier/django-redis-cache
    :alt: Travis-ci Build

A Redis cache backend for Django

Docs can be found at http://django-redis-cache.readthedocs.org/en/latest/.

Changelog
=========

2.1.2
-----

* Confirms support for Django 3.1 (no code changes required).

2.1.1
-----

* Fixes URL scheme for `rediss://`.

2.1.0
-----

* Adds support for Django 3.0.

2.0.0
-----

* Adds support for redis-py >= 3.0.
* Drops support for Redis 2.6.
* Drops support for Python 3.4.
* Removes custom ``expire`` method in lieu of Django's ``touch``.
* Removes ``CacheKey`` in favor of string literals.
* Adds testing for Django 2.2 and Python 3.7 (no code changes required).


1.8.0
-----

* Confirms support for Django 1.11, 2.0, and 2.1 (no code changes required).
* Drops support for Django < 1.11.

1.7.1
-----

* Confirms support for Django 1.9 and 1.10.


1.7.0
-----

* Drops support for Django < 1.8 and Python 3.2.

1.6.4
-----

* Adds a default timeout to ``set_many``.

1.6.3
-----

* Fixes ``get_many`` and ``set_many`` to work with empty parameters.

1.6.2
-----

* Fixes ``set_many`` to set cache key version.

1.6.1
-----

* Allows ``delete_many`` to fail silently with an empty list.

1.6.0
-----

* Adds dummy cache.

1.5.5
-----

* Cleans up ``get_or_set``.

1.5.4
-----

* Updates importlib import statement for better Django 1.9 compatibility.

1.5.3
-----

* Adds initial documentation.
* Updates function signatures to use ``DEFAULT_TIMEOUT``.
* Fixes issue with redis urls and unix_socket_path key error.

1.5.2
-----

* Adds ``SOCKET_CONNECT_TIMEOUT`` option.

1.5.1
-----

* Refactors class importing.

1.5.0
-----

* Adds ability to compress/decompress cache values using pluggable compressors
including zlib, bzip2, or a custom implementation.

1.4.0
-----

* Adds support for providing a socket timeout on the redis-py client.

1.3.0
-----

* Adds support for pluggable serializers including pickle(default), json,
msgpack, and yaml.

1.2.0
-----

* Deprecate support for Python 2.6.  The cache should still work, but tests
will fail and compatibility will not be guaranteed going forward.

**Backward incompatibilities:**

* The ``HashRing`` behavior has changed to maintain a proper keyspace balance.
This will lead to some cache misses, so be aware.

* Now requires `redis-py`_ >= 2.10.3

1.0.0
-----

* Deprecate support for django < 1.3 and redis < 2.4.  If you need support for those versions,
    pin django-redis-cache to a version less than 1.0, i.e. pip install django-redis-cache<1.0
* Application level sharding when a list of locations is provided in the settings.
* Delete keys using wildcard syntax.
* Clear cache using version to delete only keys under that namespace.
* Ability to select pickle protocol version.
* Support for Master-Slave setup
* Thundering herd protection
* Add expiration to key using `expire` command.
* Add persistence to key using `persist` command.


0.13.0
------

* Adds custom `has_key` implementation that uses Redis's `exists` command.
    This will speed `has_key` up drastically if the key under question is
    extremely large.

0.12.0
------

* Keys can now be kept alive indefinitely by setting the timeout to None,
    e.g. `cache.set('key', 'value', timeout=None)`
* Adds `ttl` method to the cache.  `cache.ttl(key)` will return the number of
    seconds before it expires or None if the key is not volatile.

0.11.0
------

* Adds support for specifying the connection pool class.
* Adds ability to set the max connections for the connection pool.


0.10.0
------

Adds Support for Python 3.3 and Django 1.5 and 1.6.  Huge thanks to Carl Meyer
for his work.

0.9.0
-----

Redis cache now allows you to use either a TCP connection or Unix domain
socket to connect to your redis server.  Using a TCP connection is useful for
when you have your redis server separate from your app server and/or within
a distributed environment.  Unix domain sockets are useful if you have your
redis server and application running on the same machine and want the fastest
possible connection.

You can now specify (optionally) what parser class you want redis-py to use
when parsing messages from the redis server.  redis-py will pick the best
parser for you implicitly, but using the ``PARSER_CLASS`` setting gives you
control and the option to roll your own parser class if you are so bold.


Requirements
============

`redis-py`_ >= 2.10.3
`redis`_ >= 2.4
`hiredis`_
`python`_ >= 2.7

1. Run ``pip install django-redis-cache``.

2. Modify your Django settings to use ``redis_cache``.

.. code:: python

    # When using TCP connections
    CACHES = {
        'default': {
            'BACKEND': 'redis_cache.RedisCache',
            'LOCATION': [
                '<host>:<port>',
                '<host>:<port>',
                '<host>:<port>',
            ],
            'OPTIONS': {
                'DB': 1,
                'PASSWORD': 'yadayada',
                'PARSER_CLASS': 'redis.connection.HiredisParser',
                'CONNECTION_POOL_CLASS': 'redis.BlockingConnectionPool',
                'CONNECTION_POOL_CLASS_KWARGS': {
                    'max_connections': 50,
                    'timeout': 20,
                },
                'MAX_CONNECTIONS': 1000,
                'PICKLE_VERSION': -1,
            },
        },
    }

    # When using unix domain sockets
    # Note: ``LOCATION`` needs to be the same as the ``unixsocket`` setting
    # in your redis.conf
    CACHES = {
        'default': {
            'BACKEND': 'redis_cache.RedisCache',
            'LOCATION': '/path/to/socket/file',
            'OPTIONS': {
                'DB': 1,
                'PASSWORD': 'yadayada',
                'PARSER_CLASS': 'redis.connection.HiredisParser',
                'PICKLE_VERSION': 2,
            },
        },
    }

    # For Master-Slave Setup, specify the host:port of the master
    # redis-server instance.
    CACHES = {
        'default': {
            'BACKEND': 'redis_cache.RedisCache',
            'LOCATION': [
                '<host>:<port>',
                '<host>:<port>',
                '<host>:<port>',
            ],
            'OPTIONS': {
                'DB': 1,
                'PASSWORD': 'yadayada',
                'PARSER_CLASS': 'redis.connection.HiredisParser',
                'PICKLE_VERSION': 2,
                'MASTER_CACHE': '<master host>:<master port>',
            },
        },
    }



Usage
=====

django-redis-cache shares the same API as django's built-in cache backends,
with a few exceptions.

``cache.delete_pattern``

Delete keys using glob-style pattern.

example::

    >>> from news.models import Story
    >>>
    >>> most_viewed = Story.objects.most_viewed()
    >>> highest_rated = Story.objects.highest_rated()
    >>> cache.set('news.stories.most_viewed', most_viewed)
    >>> cache.set('news.stories.highest_rated', highest_rated)
    >>> data = cache.get_many(['news.stories.highest_rated', 'news.stories.most_viewed'])
    >>> len(data)
    2
    >>> cache.delete_pattern('news.stores.*')
    >>> data = cache.get_many(['news.stories.highest_rated', 'news.stories.most_viewed'])
    >>> len(data)
    0

``cache.clear``

Same as django's ``cache.clear``, except that you can optionally specify a
version and all keys with that version will be deleted.  If no version is
provided, all keys are flushed from the cache.

``cache.reinsert_keys``

This helper method retrieves all keys and inserts them back into the cache.  This
is useful when changing the pickle protocol number of all the cache entries.
As of django-redis-cache < 1.0, all cache entries were pickled using version 0.
To reduce the memory footprint of the redis-server, simply run this method to
upgrade cache entries to the latest protocol.


Thundering Herd Protection
==========================

A common problem with caching is that you can sometimes get into a situation
where you have a value that takes a long time to compute or retrieve, but have
clients accessing it a lot.  For example, if you wanted to retrieve the latest
tweets from the twitter api, you probably want to cache the response for a number
of minutes so you don't exceed your rate limit.  However, when the cache entry
expires you can have mulitple clients that see there is no entry and try to
simultaneously fetch the latest results from the api.

The way to get around this problem you pass in a callable and timeout to
``get_or_set``, which will check the cache to see if you need to compute the
value.  If it does, then the cache sets a placeholder that tells future clients
to serve data from the stale cache until the new value is created.

Example::

    tweets = cache.get_or_set('tweets', twitter.get_newest, timeout=300)


Running Tests
=============

``./install_redis.sh``

``make test``

.. _redis-py: http://github.com/andymccurdy/redis-py/
.. _redis: http://github.com/antirez/redis/
.. _hiredis: http://github.com/antirez/hiredis/
.. _python: http://python.org
