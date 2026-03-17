"""
    flask_caching
    ~~~~~~~~~~~~~

    Adds cache support to your application.

    :copyright: (c) 2010 by Thadeus Burgess.
    :license: BSD, see LICENSE for more details.
"""

import base64
import functools
import hashlib
import inspect
import logging
import uuid
import warnings
from collections import OrderedDict
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from flask import current_app
from flask import Flask
from flask import request
from flask import Response
from flask import url_for
from werkzeug.utils import import_string

from flask_caching.backends.base import BaseCache
from flask_caching.backends.simplecache import SimpleCache
from flask_caching.utils import function_namespace
from flask_caching.utils import get_arg_default
from flask_caching.utils import get_arg_names
from flask_caching.utils import get_id
from flask_caching.utils import make_template_fragment_key  # noqa: F401
from flask_caching.utils import wants_args

__version__ = "2.3.1"

logger = logging.getLogger(__name__)

SUPPORTED_HASH_FUNCTIONS = [
    hashlib.sha1,
    hashlib.sha224,
    hashlib.sha256,
    hashlib.sha384,
    hashlib.sha512,
    hashlib.md5,
]


class CachedResponse(Response):
    """
    views wraped by @cached can return this (which inherits from flask.Response)
    to override the cache TTL dynamically
    """

    timeout = None

    def __init__(self, response, timeout):
        self.__dict__ = response.__dict__
        self.timeout = timeout


class Cache:
    """This class is used to control the cache objects."""

    def __init__(
        self,
        app: Optional[Flask] = None,
        with_jinja2_ext: bool = True,
        config=None,
    ) -> None:
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        self.with_jinja2_ext = with_jinja2_ext
        self.config = config

        self.source_check = None

        if app is not None:
            self.init_app(app, config)

    def init_app(self, app: Flask, config=None) -> None:
        """This is used to initialize cache with your app object"""
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        #: Ref PR #44.
        #: Do not set self.app in the case a single instance of the Cache
        #: object is being used for multiple app instances.
        #: Example use case would be Cache shipped as part of a blueprint
        #: or utility library.

        base_config = app.config.copy()
        if self.config:
            base_config.update(self.config)
        if config:
            base_config.update(config)
        config = base_config

        config.setdefault("CACHE_DEFAULT_TIMEOUT", 300)
        config.setdefault("CACHE_IGNORE_ERRORS", False)
        config.setdefault("CACHE_THRESHOLD", 500)
        config.setdefault("CACHE_KEY_PREFIX", "flask_cache_")
        config.setdefault("CACHE_MEMCACHED_SERVERS", None)
        config.setdefault("CACHE_DIR", None)
        config.setdefault("CACHE_OPTIONS", None)
        config.setdefault("CACHE_ARGS", [])
        config.setdefault("CACHE_TYPE", "null")
        config.setdefault("CACHE_NO_NULL_WARNING", False)
        config.setdefault("CACHE_SOURCE_CHECK", False)

        if config["CACHE_TYPE"] == "null" and not config["CACHE_NO_NULL_WARNING"]:
            warnings.warn(
                "Flask-Caching: CACHE_TYPE is set to null, "
                "caching is effectively disabled.",
                stacklevel=2,
            )

        if (
            config["CACHE_TYPE"] in ["filesystem", "FileSystemCache"]
            and config["CACHE_DIR"] is None
        ):
            warnings.warn(
                f"Flask-Caching: CACHE_TYPE is set to {config['CACHE_TYPE']} but no "
                "CACHE_DIR is set.",
                stacklevel=2,
            )

        self.source_check = config["CACHE_SOURCE_CHECK"]

        if self.with_jinja2_ext:
            from .jinja2ext import CacheExtension, JINJA_CACHE_ATTR_NAME

            setattr(app.jinja_env, JINJA_CACHE_ATTR_NAME, self)
            app.jinja_env.add_extension(CacheExtension)

        self._set_cache(app, config)

    def _set_cache(self, app: Flask, config) -> None:
        import_me = config["CACHE_TYPE"]
        if "." not in import_me:
            plain_name_used = True
            import_me = "flask_caching.backends." + import_me
        else:
            plain_name_used = False

        cache_factory = import_string(import_me)
        cache_args = config["CACHE_ARGS"][:]
        cache_options = {"default_timeout": config["CACHE_DEFAULT_TIMEOUT"]}

        if isinstance(cache_factory, type) and issubclass(cache_factory, BaseCache):
            cache_factory = cache_factory.factory
        elif plain_name_used:
            warnings.warn(
                "Using the initialization functions in flask_caching.backend "
                "is deprecated.  Use the a full path to backend classes "
                "directly.",
                category=DeprecationWarning,
                stacklevel=2,
            )

        if config["CACHE_OPTIONS"]:
            cache_options.update(config["CACHE_OPTIONS"])

        if not hasattr(app, "extensions"):
            app.extensions = {}

        app.extensions.setdefault("cache", {})
        app.extensions["cache"][self] = cache_factory(
            app, config, cache_args, cache_options
        )
        self.app = app

    def _call_fn(self, fn, *args, **kwargs):
        ensure_sync = getattr(self.app, "ensure_sync", None)
        if ensure_sync is not None:
            return ensure_sync(fn)(*args, **kwargs)
        return fn(*args, **kwargs)

    @property
    def cache(self) -> SimpleCache:
        app = current_app or self.app
        return app.extensions["cache"][self]

    def get(self, *args, **kwargs) -> Any:
        """Proxy function for internal cache object."""
        return self.cache.get(*args, **kwargs)

    def has(self, *args, **kwargs) -> bool:
        """Proxy function for internal cache object."""
        return self.cache.has(*args, **kwargs)

    def set(self, *args, **kwargs) -> Optional[bool]:
        """Proxy function for internal cache object."""
        return self.cache.set(*args, **kwargs)

    def add(self, *args, **kwargs) -> bool:
        """Proxy function for internal cache object."""
        return self.cache.add(*args, **kwargs)

    def delete(self, *args, **kwargs) -> bool:
        """Proxy function for internal cache object."""
        return self.cache.delete(*args, **kwargs)

    def delete_many(self, *args, **kwargs) -> List[str]:
        """Proxy function for internal cache object."""
        return self.cache.delete_many(*args, **kwargs)

    def clear(self) -> bool:
        """Proxy function for internal cache object."""
        return self.cache.clear()

    def get_many(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.get_many(*args, **kwargs)

    def set_many(self, *args, **kwargs) -> List[Any]:
        """Proxy function for internal cache object."""
        return self.cache.set_many(*args, **kwargs)

    def get_dict(self, *args, **kwargs) -> Dict[str, Any]:
        """Proxy function for internal cache object."""
        return self.cache.get_dict(*args, **kwargs)

    def unlink(self, *args, **kwargs) -> List[str]:
        """Proxy function for internal cache object
        only support Redis
        """
        unlink = getattr(self.cache, "unlink", None)
        if unlink is not None and callable(unlink):
            return unlink(*args, **kwargs)
        return self.delete_many(*args, **kwargs)

    def cached(
        self,
        timeout: Optional[int] = None,
        key_prefix: str = "view/%s",
        unless: Optional[Callable] = None,
        forced_update: Optional[Callable] = None,
        response_filter: Optional[Callable] = None,
        query_string: bool = False,
        hash_method: Callable = hashlib.md5,
        cache_none: bool = False,
        make_cache_key: Optional[Callable] = None,
        source_check: Optional[bool] = None,
        response_hit_indication: Optional[bool] = False,
    ) -> Callable:
        """Decorator. Use this to cache a function. By default the cache key
        is `view/request.path`. You are able to use this decorator with any
        function by changing the `key_prefix`. If the token `%s` is located
        within the `key_prefix` then it will replace that with `request.path`

        Example::

            # An example view function
            @cache.cached(timeout=50)
            def big_foo():
                return big_bar_calc()

            # An example misc function to cache.
            @cache.cached(key_prefix='MyCachedList')
            def get_list():
                return [random.randrange(0, 1) for i in range(50000)]

            my_list = get_list()

        .. note::

            You MUST have a request context to actually called any functions
            that are cached.

        .. versionadded:: 0.4
            The returned decorated function now has three function attributes
            assigned to it. These attributes are readable/writable.

                **uncached**
                    The original undecorated function

                **cache_timeout**
                    The cache timeout value for this function. For a
                    custom value to take affect, this must be set before the
                    function is called.

                **make_cache_key**
                    A function used in generating the cache_key used.

                    readable and writable

        :param timeout: Default None. If set to an integer, will cache for that
                        amount of time. Unit of time is in seconds.

        :param key_prefix: Default 'view/%(request.path)s'. Beginning key to .
                           use for the cache key. `request.path` will be the
                           actual request path, or in cases where the
                           `make_cache_key`-function is called from other
                           views it will be the expected URL for the view
                           as generated by Flask's `url_for()`.

                           .. versionadded:: 0.3.4
                               Can optionally be a callable which takes
                               no arguments but returns a string that will
                               be used as the cache_key.

        :param unless: Default None. Cache will *always* execute the caching
                       facilities unless this callable is true.
                       This will bypass the caching entirely.

        :param forced_update: Default None. If this callable is true,
                              cache value will be updated regardless cache
                              is expired or not. Useful for background
                              renewal of cached functions.

        :param response_filter: Default None. If not None, the callable is
                                invoked after the cached function evaluation,
                                and is given one argument, the response
                                content. If the callable returns False, the
                                content will not be cached. Useful to prevent
                                caching of code 500 responses.

        :param query_string: Default False. When True, the cache key
                             used will be the result of hashing the
                             ordered query string parameters. This
                             avoids creating different caches for
                             the same query just because the parameters
                             were passed in a different order. See
                             _make_cache_key_query_string() for more
                             details.

        :param hash_method: Default hashlib.md5. The hash method used to
                            generate the keys for cached results.
        :param cache_none: Default False. If set to True, add a key exists
                           check when cache.get returns None. This will likely
                           lead to wrongly returned None values in concurrent
                           situations and is not recommended to use.
        :param make_cache_key: Default None. If set to a callable object,
                           it will be called to generate the cache key

        :param source_check: Default None. If None will use the value set by
                             CACHE_SOURCE_CHECK.
                             If True, include the function's source code in the
                             hash to avoid using cached values when the source
                             code has changed and the input values remain the
                             same. This ensures that the cache_key will be
                             formed with the function's source code hash in
                             addition to other parameters that may be included
                             in the formation of the key.

        :param response_hit_indication: Default False.
                             If True, it will add to response header field 'hit_cache'
                             if used cache.
        """

        def decorator(f):
            @functools.wraps(f)
            def decorated_function(*args, **kwargs):
                #: Bypass the cache entirely.
                if self._bypass_cache(unless, f, *args, **kwargs):
                    return self._call_fn(f, *args, **kwargs)

                nonlocal source_check
                if source_check is None:
                    source_check = self.source_check

                try:
                    if make_cache_key is not None and callable(make_cache_key):
                        cache_key = make_cache_key(*args, **kwargs)
                    else:
                        cache_key = decorated_function.make_cache_key(
                            *args, use_request=True, **kwargs
                        )

                    if (
                        callable(forced_update)
                        and (
                            forced_update(*args, **kwargs)
                            if wants_args(forced_update)
                            else forced_update()
                        )
                        is True
                    ):
                        rv = None
                        found = False
                    else:
                        rv = self.cache.get(cache_key)
                        found = True

                        # If the value returned by cache.get() is None, it
                        # might be because the key is not found in the cache
                        # or because the cached value is actually None
                        if rv is None:
                            # If we're sure we don't need to cache None values
                            # (cache_none=False), don't bother checking for
                            # key existence, as it can lead to false positives
                            # if a concurrent call already cached the
                            # key between steps. This would cause us to
                            # return None when we shouldn't
                            if not cache_none:
                                found = False
                            else:
                                found = self.cache.has(cache_key)
                except Exception:
                    if self.app.debug:
                        raise
                    logger.exception("Exception possibly due to cache backend.")
                    return self._call_fn(f, *args, **kwargs)
                if found and self.app.debug:
                    logger.info(f"Cache used for key: {cache_key}")
                if response_hit_indication:

                    def apply_caching(response):
                        if found:
                            response.headers["hit_cache"] = found
                        return response

                    self.app.after_request_funcs[None].append(apply_caching)

                if not found:
                    rv = self._call_fn(f, *args, **kwargs)
                    if inspect.isgenerator(rv):
                        rv = [val for val in rv]

                    if response_filter is None or response_filter(rv):
                        cache_timeout = decorated_function.cache_timeout
                        if isinstance(rv, CachedResponse):
                            cache_timeout = rv.timeout or cache_timeout
                        try:
                            self.cache.set(
                                cache_key,
                                rv,
                                timeout=cache_timeout,
                            )
                        except Exception:
                            if self.app.debug:
                                raise
                            logger.exception("Exception possibly due to cache backend.")
                return rv

            def default_make_cache_key(*args, **kwargs):
                # Convert non-keyword arguments (which is the way
                # `make_cache_key` expects them) to keyword arguments
                # (the way `url_for` expects them)
                argspec_args = inspect.getfullargspec(f).args

                for arg_name, arg in zip(argspec_args, args):
                    kwargs[arg_name] = arg

                use_request = kwargs.pop("use_request", False)
                return _make_cache_key(args, kwargs, use_request=use_request)

            def _make_cache_key_query_string():
                """Create consistent keys for query string arguments.

                Produces the same cache key regardless of argument order, e.g.,
                both `?limit=10&offset=20` and `?offset=20&limit=10` will
                always produce the same exact cache key.

                If func is provided and is callable it will be used to hash
                the function's source code and include it in the cache key.
                This will only be done is source_check is True.
                """

                # Create a tuple of (key, value) pairs, where the key is the
                # argument name and the value is its respective value. Order
                # this tuple by key. Doing this ensures the cache key created
                # is always the same for query string args whose keys/values
                # are the same, regardless of the order in which they are
                # provided.

                args_as_sorted_tuple = tuple(
                    sorted(pair for pair in request.args.items(multi=True))
                )
                # ... now hash the sorted (key, value) tuple so it can be
                # used as a key for cache. Turn them into bytes so that the
                # hash function will accept them
                args_as_bytes = str(args_as_sorted_tuple).encode()
                cache_hash = hash_method(args_as_bytes)

                # Use the source code if source_check is True and update the
                # cache_hash before generating the hashing and using it in
                # cache_key
                if source_check and callable(f):
                    func_source_code = inspect.getsource(f)
                    cache_hash.update(func_source_code.encode("utf-8"))

                cache_hash = str(cache_hash.hexdigest())

                cache_key = request.path + cache_hash

                return cache_key

            def _make_cache_key(args, kwargs, use_request) -> str:
                if query_string:
                    return _make_cache_key_query_string()
                else:
                    if callable(key_prefix):
                        cache_key = key_prefix()
                    elif "%s" in key_prefix:
                        if use_request:
                            cache_key = key_prefix % request.path
                        else:
                            cache_key = key_prefix % url_for(f.__name__, **kwargs)
                    else:
                        cache_key = key_prefix

                if source_check and callable(f):
                    func_source_code = inspect.getsource(f)
                    func_source_hash = hash_method(func_source_code.encode("utf-8"))
                    func_source_hash = str(func_source_hash.hexdigest())

                    cache_key += func_source_hash

                return cache_key

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = default_make_cache_key

            return decorated_function

        return decorator

    def _memvname(self, funcname: str) -> str:
        return funcname + "_memver"

    def _memoize_make_version_hash(self) -> str:
        return base64.b64encode(uuid.uuid4().bytes)[:6].decode("utf-8")

    def _memoize_version(
        self,
        f: Callable,
        args: Optional[Any] = None,
        kwargs=None,
        reset: bool = False,
        delete: bool = False,
        timeout: Optional[int] = None,
        forced_update: Optional[Union[bool, Callable]] = False,
        args_to_ignore: Optional[Any] = None,
    ) -> Union[Tuple[str, str], Tuple[str, None]]:
        """Updates the hash version associated with a memoized function or
        method.
        """
        fname, instance_fname = function_namespace(f, args=args)
        version_key = self._memvname(fname)
        fetch_keys = [version_key]

        args_to_ignore = args_to_ignore or []
        if "self" in args_to_ignore:
            instance_fname = None

        if instance_fname:
            instance_version_key = self._memvname(instance_fname)
            fetch_keys.append(instance_version_key)

        # Only delete the per-instance version key or per-function version
        # key but not both.
        if delete:
            self.cache.delete_many(fetch_keys[-1])
            return fname, None

        version_data_list = list(self.cache.get_many(*fetch_keys))
        dirty = False

        if (
            callable(forced_update)
            and (
                forced_update(*(args or ()), **(kwargs or {}))
                if wants_args(forced_update)
                else forced_update()
            )
            is True
        ):
            # Mark key as dirty to update its TTL
            dirty = True

        if version_data_list[0] is None:
            version_data_list[0] = self._memoize_make_version_hash()
            dirty = True

        if instance_fname and version_data_list[1] is None:
            version_data_list[1] = self._memoize_make_version_hash()
            dirty = True

        # Only reset the per-instance version or the per-function version
        # but not both.
        if reset:
            fetch_keys = fetch_keys[-1:]
            version_data_list = [self._memoize_make_version_hash()]
            dirty = True

        if dirty:
            self.cache.set_many(
                dict(zip(fetch_keys, version_data_list)), timeout=timeout
            )

        return fname, "".join(version_data_list)

    def _memoize_make_cache_key(
        self,
        make_name: Optional[Callable] = None,
        timeout: Optional[Callable] = None,
        forced_update: bool = False,
        hash_method: Callable = hashlib.md5,
        source_check: Optional[bool] = False,
        args_to_ignore: Optional[Any] = None,
    ) -> Callable:
        """Function used to create the cache_key for memoized functions."""

        def make_cache_key(f, *args, **kwargs):
            _timeout = getattr(timeout, "cache_timeout", timeout)
            fname, version_data = self._memoize_version(
                f,
                args=args,
                kwargs=kwargs,
                timeout=_timeout,
                forced_update=forced_update,
                args_to_ignore=args_to_ignore,
            )

            #: this should have to be after version_data, so that it
            #: does not break the delete_memoized functionality.
            altfname = make_name(fname) if callable(make_name) else fname

            if callable(f):
                keyargs, keykwargs = self._memoize_kwargs_to_args(
                    f, *args, **kwargs, args_to_ignore=args_to_ignore
                )
            else:
                keyargs, keykwargs = args, kwargs

            updated = f"{altfname}{keyargs}{keykwargs}"

            cache_key = hash_method()
            cache_key.update(updated.encode("utf-8"))

            # Use the source code if source_check is True and update the
            # cache_key with the function's source.
            if source_check and callable(f):
                func_source_code = inspect.getsource(f)
                cache_key.update(func_source_code.encode("utf-8"))

            cache_key = base64.b64encode(cache_key.digest())[:16]
            cache_key = cache_key.decode("utf-8")
            cache_key += version_data

            return cache_key

        return make_cache_key

    def _memoize_kwargs_to_args(self, f: Callable, *args, **kwargs) -> Any:
        #: Inspect the arguments to the function
        #: This allows the memoization to be the same
        #: whether the function was called with
        #: 1, b=2 is equivalent to a=1, b=2, etc.
        new_args = []
        arg_num = 0
        args_to_ignore = kwargs.pop("args_to_ignore", None) or []

        # If the function uses VAR_KEYWORD type of parameters,
        # we need to pass these further
        kw_keys_remaining = [key for key in kwargs.keys() if key not in args_to_ignore]
        arg_names = get_arg_names(f)
        args_len = len(arg_names)

        for i in range(args_len):
            arg_default = get_arg_default(f, i)
            if arg_names[i] in args_to_ignore:
                arg = None
                arg_num += 1
            elif i == 0 and arg_names[i] in ("self", "cls"):
                #: use the id func of the class instance
                #: this supports instance methods for
                #: the memoized functions, giving more
                #: flexibility to developers
                arg = get_id(args[0])
                arg_num += 1
            elif arg_names[i] in kwargs:
                arg = kwargs[arg_names[i]]
                kw_keys_remaining.pop(kw_keys_remaining.index(arg_names[i]))
            elif arg_num < len(args):
                arg = args[arg_num]
                arg_num += 1
            elif arg_default:
                arg = arg_default
                arg_num += 1
            else:
                arg = None
                arg_num += 1

            #: Attempt to convert all arguments to a
            #: hash/id or a representation?
            #: Not sure if this is necessary, since
            #: using objects as keys gets tricky quickly.
            # if hasattr(arg, '__class__'):
            #     try:
            #         arg = hash(arg)
            #     except:
            #         arg = get_id(arg)

            #: Or what about a special __cacherepr__ function
            #: on an object, this allows objects to act normal
            #: upon inspection, yet they can define a representation
            #: that can be used to make the object unique in the
            #: cache key. Given that a case comes across that
            #: an object "must" be used as a cache key
            # if hasattr(arg, '__cacherepr__'):
            #     arg = arg.__cacherepr__

            new_args.append(arg)

        new_args.extend(args[len(arg_names) :])
        return (
            tuple(new_args),
            OrderedDict(
                sorted((k, v) for k, v in kwargs.items() if k in kw_keys_remaining)
            ),
        )

    def _bypass_cache(
        self, unless: Optional[Callable], f: Callable, *args, **kwargs
    ) -> bool:
        """Determines whether or not to bypass the cache by calling unless().
        Supports both unless() that takes in arguments and unless()
        that doesn't.
        """
        bypass_cache = False

        if callable(unless):
            argspec = inspect.getfullargspec(unless)
            has_args = len(argspec.args) > 0 or argspec.varargs or argspec.varkw

            # If unless() takes args, pass them in.
            if has_args:
                if unless(f, *args, **kwargs) is True:
                    bypass_cache = True
            elif unless() is True:
                bypass_cache = True

        return bypass_cache

    def memoize(
        self,
        timeout: Optional[int] = None,
        make_name: Optional[Callable] = None,
        unless: Optional[Callable] = None,
        forced_update: Optional[Callable] = None,
        response_filter: Optional[Callable] = None,
        hash_method: Callable = hashlib.md5,
        cache_none: bool = False,
        source_check: Optional[bool] = None,
        args_to_ignore: Optional[Any] = None,
    ) -> Callable:
        """Use this to cache the result of a function, taking its arguments
        into account in the cache key.

        Information on
        `Memoization <http://en.wikipedia.org/wiki/Memoization>`_.

        Example::

            @cache.memoize(timeout=50)
            def big_foo(a, b):
                return a + b + random.randrange(0, 1000)

        .. code-block:: pycon

            >>> big_foo(5, 2)
            753
            >>> big_foo(5, 3)
            234
            >>> big_foo(5, 2)
            753

        .. versionadded:: 0.4
            The returned decorated function now has three function attributes
            assigned to it.

                **uncached**
                    The original undecorated function. readable only

                **cache_timeout**
                    The cache timeout value for this function.
                    For a custom value to take affect, this must be
                    set before the function is called.

                    readable and writable

                **make_cache_key**
                    A function used in generating the cache_key used.

                    readable and writable


        :param timeout: Default None. If set to an integer, will cache for that
                        amount of time. Unit of time is in seconds.
        :param make_name: Default None. If set this is a function that accepts
                          a single argument, the function name, and returns a
                          new string to be used as the function name.
                          If not set then the function name is used.
        :param unless: Default None. Cache will *always* execute the caching
                       facilities unless this callable is true.
                       This will bypass the caching entirely.
        :param forced_update: Default None. If this callable is true,
                              cache value will be updated regardless cache
                              is expired or not. Useful for background
                              renewal of cached functions.
        :param response_filter: Default None. If not None, the callable is
                                invoked after the cached funtion evaluation,
                                and is given one arguement, the response
                                content. If the callable returns False, the
                                content will not be cached. Useful to prevent
                                caching of code 500 responses.
        :param hash_method: Default hashlib.md5. The hash method used to
                            generate the keys for cached results.
        :param cache_none: Default False. If set to True, add a key exists
                           check when cache.get returns None. This will likely
                           lead to wrongly returned None values in concurrent
                           situations and is not recommended to use.

        :param source_check: Default None. If None will use the value set by
                             CACHE_SOURCE_CHECK.
                             If True, include the function's source code in the
                             hash to avoid using cached values when the source
                             code has changed and the input values remain the
                             same. This ensures that the cache_key will be
                             formed with the function's source code hash in
                             addition to other parameters that may be included
                             in the formation of the key.
        :param args_to_ignore: List of arguments that will be ignored while
                               generating the cache key. Default to None.
                               This means that those arguments may change
                               without affecting the cache value that will be
                               returned.

        .. versionadded:: 0.5
            params ``make_name``, ``unless``

        .. versionadded:: 1.10
            params ``args_to_ignore``
        """

        def memoize(f):
            @functools.wraps(f)
            def decorated_function(*args, **kwargs):
                #: bypass cache
                if self._bypass_cache(unless, f, *args, **kwargs):
                    return self._call_fn(f, *args, **kwargs)

                nonlocal source_check
                if source_check is None:
                    source_check = self.source_check

                try:
                    cache_key = decorated_function.make_cache_key(f, *args, **kwargs)

                    if (
                        callable(forced_update)
                        and (
                            forced_update(*args, **kwargs)
                            if wants_args(forced_update)
                            else forced_update()
                        )
                        is True
                    ):
                        rv = None
                        found = False
                    else:
                        rv = self.cache.get(cache_key)
                        found = True

                        # If the value returned by cache.get() is None, it
                        # might be because the key is not found in the cache
                        # or because the cached value is actually None
                        if rv is None:
                            # If we're sure we don't need to cache None values
                            # (cache_none=False), don't bother checking for
                            # key existence, as it can lead to false positives
                            # if a concurrent call already cached the
                            # key between steps. This would cause us to
                            # return None when we shouldn't
                            if not cache_none:
                                found = False
                            else:
                                found = self.cache.has(cache_key)
                except Exception:
                    if self.app.debug:
                        raise
                    logger.exception("Exception possibly due to cache backend.")
                    return self._call_fn(f, *args, **kwargs)

                if not found:
                    rv = self._call_fn(f, *args, **kwargs)
                    if inspect.isgenerator(rv):
                        rv = [val for val in rv]

                    if response_filter is None or response_filter(rv):
                        try:
                            self.cache.set(
                                cache_key,
                                rv,
                                timeout=decorated_function.cache_timeout,
                            )
                        except Exception:
                            if self.app.debug:
                                raise
                            logger.exception("Exception possibly due to cache backend.")
                return rv

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = self._memoize_make_cache_key(
                make_name=make_name,
                timeout=decorated_function,
                forced_update=forced_update,
                hash_method=hash_method,
                source_check=source_check,
                args_to_ignore=args_to_ignore,
            )
            decorated_function.delete_memoized = lambda: self.delete_memoized(f)

            return decorated_function

        return memoize

    def delete_memoized(self, f, *args, **kwargs) -> None:
        """Deletes the specified functions caches, based by given parameters.
        If parameters are given, only the functions that were memoized
        with them will be erased. Otherwise all versions of the caches
        will be forgotten.

        Example::

            @cache.memoize(50)
            def random_func():
                return random.randrange(1, 50)

            @cache.memoize()
            def param_func(a, b):
                return a+b+random.randrange(1, 50)

        .. code-block:: pycon

            >>> random_func()
            43
            >>> random_func()
            43
            >>> cache.delete_memoized(random_func)
            >>> random_func()
            16
            >>> param_func(1, 2)
            32
            >>> param_func(1, 2)
            32
            >>> param_func(2, 2)
            47
            >>> cache.delete_memoized(param_func, 1, 2)
            >>> param_func(1, 2)
            13
            >>> param_func(2, 2)
            47

        Delete memoized is also smart about instance methods vs class methods.

        When passing a instancemethod, it will only clear the cache related
        to that instance of that object. (object uniqueness can be overridden
        by defining the __repr__ method, such as user id).

        When passing a classmethod, it will clear all caches related across
        all instances of that class.

        Example::

            class Adder(object):
                @cache.memoize()
                def add(self, b):
                    return b + random.random()

        .. code-block:: pycon

            >>> adder1 = Adder()
            >>> adder2 = Adder()
            >>> adder1.add(3)
            3.23214234
            >>> adder2.add(3)
            3.60898509
            >>> cache.delete_memoized(adder1.add)
            >>> adder1.add(3)
            3.01348673
            >>> adder2.add(3)
            3.60898509
            >>> cache.delete_memoized(Adder.add)
            >>> adder1.add(3)
            3.53235667
            >>> adder2.add(3)
            3.72341788

        :param fname: The memoized function.
        :param \\*args: A list of positional parameters used with
                       memoized function.
        :param \\**kwargs: A dict of named parameters used with
                          memoized function.

        .. note::

            Flask-Caching uses inspect to order kwargs into positional args when
            the function is memoized. If you pass a function reference into
            ``fname``, Flask-Caching will be able to place the args/kwargs in
            the proper order, and delete the positional cache.

            However, if ``delete_memoized`` is just called with the name of the
            function, be sure to pass in potential arguments in the same order
            as defined in your function as args only, otherwise Flask-Caching
            will not be able to compute the same cache key and delete all
            memoized versions of it.

        .. note::

            Flask-Caching maintains an internal random version hash for
            the function. Using delete_memoized will only swap out
            the version hash, causing the memoize function to recompute
            results and put them into another key.

            This leaves any computed caches for this memoized function within
            the caching backend.

            It is recommended to use a very high timeout with memoize if using
            this function, so that when the version hash is swapped, the old
            cached results would eventually be reclaimed by the caching
            backend.
        """
        if not callable(f):
            raise TypeError(
                "Deleting messages by relative name is not supported, please "
                "use a function reference."
            )

        if not (args or kwargs):
            self._memoize_version(f, reset=True)
        else:
            cache_key = f.make_cache_key(f.uncached, *args, **kwargs)
            self.cache.delete(cache_key)

    def delete_memoized_verhash(self, f: Callable, *args) -> None:
        """Delete the version hash associated with the function.

        .. warning::

            Performing this operation could leave keys behind that have
            been created with this version hash. It is up to the application
            to make sure that all keys that may have been created with this
            version hash at least have timeouts so they will not sit orphaned
            in the cache backend.
        """
        if not callable(f):
            raise TypeError(
                "Deleting messages by relative name is not supported, please"
                "use a function reference."
            )

        self._memoize_version(f, delete=True)
