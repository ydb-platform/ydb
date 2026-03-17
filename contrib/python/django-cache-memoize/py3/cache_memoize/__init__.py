from functools import wraps
import itertools
import json
import inspect

import hashlib
from urllib.parse import quote

from django.db import models
from django.core.cache import caches, DEFAULT_CACHE_ALIAS
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from django.utils.encoding import force_bytes

MARKER = object()


def cache_memoize(
    timeout=DEFAULT_TIMEOUT,
    prefix=None,
    extra=None,
    args_rewrite=None,
    hit_callable=None,
    miss_callable=None,
    key_generator_callable=None,
    store_result=True,
    cache_exceptions=(),
    cache_alias=DEFAULT_CACHE_ALIAS,
):
    """Decorator for memoizing function calls where we use the
    "local cache" to store the result.

    :arg int timeout: Number of seconds to store the result if not None
    :arg string prefix: If None becomes the function name.
    :arg extra: Optional callable or serializable structure of key
    components cache should vary on.
    :arg function args_rewrite: Callable that rewrites the args first useful
    if your function needs nontrivial types but you know a simple way to
    re-represent them for the sake of the cache key.
    :arg function hit_callable: Gets executed if key was in cache.
    :arg function miss_callable: Gets executed if key was *not* in cache.
    :arg key_generator_callable: Custom cache key name generator.
    :arg bool store_result: If you know the result is not important, just
    that the cache blocked it from running repeatedly, set this to False.
    :arg Exception cache_exceptions: Accepts an Exception or a tuple of
    Exceptions. If the cached function raises any of these exceptions is the
    exception cached and raised as normal. Subsequent cached calls will
    immediately re-raise the exception and the function will not be executed.
    this tuple will be cached, all other will be propagated.
    :arg string cache_alias: The cache alias to use; defaults to 'default'.

    Usage::

        @cache_memoize(
            300,  # 5 min
            args_rewrite=lambda user: user.email,
            hit_callable=lambda: print("Cache hit!"),
            miss_callable=lambda: print("Cache miss :("),
        )
        def hash_user_email(user):
            dk = hashlib.pbkdf2_hmac('sha256', user.email, b'salt', 100000)
            return binascii.hexlify(dk)

    Or, when you don't actually need the result, useful if you know it's not
    valuable to store the execution result::

        @cache_memoize(
            300,  # 5 min
            store_result=False,
        )
        def send_email(email):
            somelib.send(email, subject="You rock!", ...)

    Also, whatever you do where things get cached, you can undo that.
    For example::

        @cache_memoize(100)
        def callmeonce(arg1):
            print(arg1)

        callmeonce('peter')  # will print 'peter'
        callmeonce('peter')  # nothing printed
        callmeonce.invalidate('peter')
        callmeonce('peter')  # will print 'peter'

    Suppose you know for good reason you want to bypass the cache and
    really let the decorator let you through you can set one extra
    keyword argument called `_refresh`. For example::

        @cache_memoize(100)
        def callmeonce(arg1):
            print(arg1)

        callmeonce('peter')                 # will print 'peter'
        callmeonce('peter')                 # nothing printed
        callmeonce('peter', _refresh=True)  # will print 'peter'

    If your cache depends on external state you can provide `extra` values::

        @cache_memoize(100, extra={'version': 2})
        def callmeonce(arg1):
            print(arg1)

    An `extra` argument can be callable or any serializable structure::

        @cache_memoize(100, extra=lambda req: req.user.is_staff)
        def callmeonce(arg1):
            print(arg1)
    """

    if args_rewrite is None:

        def noop(*args):
            return args

        args_rewrite = noop

    def obj_key(obj):
        if isinstance(obj, models.Model):
            return "%s.%s.%s" % (obj._meta.app_label, obj._meta.model_name, obj.pk)
        elif hasattr(obj, "build_absolute_uri"):
            return obj.build_absolute_uri()
        elif inspect.isfunction(obj):
            factors = [obj.__module__, obj.__name__]
            return factors
        else:
            return str(obj)

    def decorator(func):
        def _default_make_cache_key(*args, **kwargs):
            cache_key = ":".join(
                itertools.chain(
                    (quote(str(x)) for x in args_rewrite(*args)),
                    (
                        "{}={}".format(quote(k), quote(str(v)))
                        for k, v in sorted(kwargs.items())
                    ),
                )
            )
            prefix_ = prefix or ".".join((func.__module__ or "", func.__qualname__))
            extra_val = json.dumps(
                extra(*args, **kwargs) if callable(extra) else extra,
                sort_keys=True,
                default=obj_key,
            )
            return hashlib.md5(
                force_bytes("cache_memoize" + prefix_ + cache_key + extra_val)
            ).hexdigest()

        _make_cache_key = key_generator_callable or _default_make_cache_key

        @wraps(func)
        def inner(*args, **kwargs):
            # The cache backend is fetched here (not in the outer decorator scope)
            # to guarantee thread-safety at runtime.
            cache = caches[cache_alias]
            # The cache key string should never be dependent on special keyword
            # arguments like _refresh. So extract it into a variable as soon as
            # possible.
            _refresh = bool(kwargs.pop("_refresh", False))
            cache_key = _make_cache_key(*args, **kwargs)
            if _refresh:
                result = MARKER
            else:
                result = cache.get(cache_key, MARKER)
            if result is MARKER:
                # If the function all raises an exception we want to cache,
                # catch it, else let it propagate.
                try:
                    result = func(*args, **kwargs)
                except cache_exceptions as exception:
                    result = exception

                if not store_result:
                    # Then the result isn't valuable/important to store but
                    # we want to store something. Just to remember that
                    # it has be done.
                    cache.set(cache_key, True, timeout)
                else:
                    cache.set(cache_key, result, timeout)
                if miss_callable:
                    miss_callable(*args, **kwargs)
            elif hit_callable:
                hit_callable(*args, **kwargs)

            # If the result is an exception we've caught and cached, raise it
            # in the end as to not change the API of the function we're caching.
            if isinstance(result, Exception):
                raise result
            return result

        def invalidate(*args, **kwargs):
            # The cache backend is fetched here (not in the outer decorator scope)
            # to guarantee thread-safety at runtime.
            cache = caches[cache_alias]
            kwargs.pop("_refresh", None)
            cache_key = _make_cache_key(*args, **kwargs)
            cache.delete(cache_key)

        def get_cache_key(*args, **kwargs):
            kwargs.pop("_refresh", None)
            return _make_cache_key(*args, **kwargs)

        inner.invalidate = invalidate
        inner.get_cache_key = get_cache_key
        return inner

    return decorator
