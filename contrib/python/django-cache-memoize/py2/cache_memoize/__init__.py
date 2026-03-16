from functools import wraps

import hashlib
from django.core.cache import caches, DEFAULT_CACHE_ALIAS

from django.utils.encoding import force_text, force_bytes

MARKER = object()


def cache_memoize(
    timeout,
    prefix=None,
    args_rewrite=None,
    hit_callable=None,
    miss_callable=None,
    key_generator_callable=None,
    store_result=True,
    cache_alias=DEFAULT_CACHE_ALIAS,
):
    """Decorator for memoizing function calls where we use the
    "local cache" to store the result.

    :arg int timeout: Number of seconds to store the result if not None
    :arg string prefix: If None becomes the function name.
    :arg function args_rewrite: Callable that rewrites the args first useful
    if your function needs nontrivial types but you know a simple way to
    re-represent them for the sake of the cache key.
    :arg function hit_callable: Gets executed if key was in cache.
    :arg function miss_callable: Gets executed if key was *not* in cache.
    :arg key_generator_callable: Custom cache key name generator.
    :arg bool store_result: If you know the result is not important, just
    that the cache blocked it from running repeatedly, set this to False.
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

    """

    if args_rewrite is None:

        def noop(*args):
            return args

        args_rewrite = noop

    def decorator(func):
        def _default_make_cache_key(*args, **kwargs):
            cache_key = ":".join(
                [force_text(x) for x in args_rewrite(*args)]
                + [force_text("{}={}".format(k, v)) for k, v in kwargs.items()]
            )
            return hashlib.md5(
                force_bytes("cache_memoize" + (prefix or func.__name__) + cache_key)
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
                result = func(*args, **kwargs)
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
            return result

        def invalidate(*args, **kwargs):
            # The cache backend is fetched here (not in the outer decorator scope)
            # to guarantee thread-safety at runtime.
            cache = caches[cache_alias]
            kwargs.pop("_refresh", None)
            cache_key = _make_cache_key(*args, **kwargs)
            cache.delete(cache_key)

        inner.invalidate = invalidate
        return inner

    return decorator
