# -*- coding: utf-8 -*-
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
import string
import uuid
import warnings
from collections import OrderedDict

from flask import current_app, request, url_for
from werkzeug.utils import import_string

from flask_caching._compat import PY2, iteritems

__version__ = "1.7.2"

logger = logging.getLogger(__name__)

TEMPLATE_FRAGMENT_KEY_TEMPLATE = "_template_fragment_cache_%s%s"
SUPPORTED_HASH_FUNCTIONS = [
    hashlib.sha1,
    hashlib.sha224,
    hashlib.sha256,
    hashlib.sha384,
    hashlib.sha512,
    hashlib.md5,
]

# Used to remove control characters and whitespace from cache keys.
valid_chars = set(string.ascii_letters + string.digits + "_.")
delchars = "".join(c for c in map(chr, range(256)) if c not in valid_chars)
if PY2:
    null_control = (None, delchars)
else:
    null_control = (dict((k, None) for k in delchars),)


def get_arg_names(f):
    """Return arguments of function

    :param f:
    :return: String list of arguments
    """
    try:
        # Python >= 3.3
        sig = inspect.signature(f)
        return [
            parameter.name
            for parameter in sig.parameters.values()
            if parameter.kind == parameter.POSITIONAL_OR_KEYWORD
        ]
    except AttributeError:
        try:
            # Python >= 3.0
            return inspect.getfullargspec(f).args
        except AttributeError:
            return inspect.getargspec(f).args


def get_arg_default(f, position):
    try:
        # Python >= 3.3
        sig = inspect.signature(f)
        arg = list(sig.parameters.values())[position]
        arg_def = arg.default
        return arg_def if arg_def != inspect.Parameter.empty else None
    except AttributeError:
        try:
            spec = inspect.getfullargspec(f)
        except AttributeError:
            spec = inspect.getargspec(f)

        args_len = len(spec.args)
        if spec.defaults and abs(position - args_len) <= len(spec.defaults):
            return spec.defaults[position - args_len]
        else:
            return None


def function_namespace(f, args=None):
    """Attempts to returns unique namespace for function"""
    m_args = get_arg_names(f)

    instance_token = None

    instance_self = getattr(f, "__self__", None)

    if instance_self and not inspect.isclass(instance_self):
        instance_token = repr(f.__self__)
    elif m_args and m_args[0] == "self" and args:
        instance_token = repr(args[0])

    module = f.__module__

    if m_args and m_args[0] == "cls" and not inspect.isclass(args[0]):
        raise ValueError(
            "When using `delete_memoized` on a "
            "`@classmethod` you must provide the "
            "class as the first argument"
        )

    if hasattr(f, "__qualname__"):
        name = f.__qualname__
    else:
        klass = getattr(f, "__self__", None)

        if klass and not inspect.isclass(klass):
            klass = klass.__class__

        if not klass:
            klass = getattr(f, "im_class", None)

        if not klass:
            if m_args and args:
                if m_args[0] == "self":
                    klass = args[0].__class__
                elif m_args[0] == "cls":
                    klass = args[0]

        if klass:
            name = klass.__name__ + "." + f.__name__
        else:
            name = f.__name__

    ns = ".".join((module, name))
    ns = ns.translate(*null_control)

    if instance_token:
        ins = ".".join((module, name, instance_token))
        ins = ins.translate(*null_control)
    else:
        ins = None

    return ns, ins


def make_template_fragment_key(fragment_name, vary_on=[]):
    """Make a cache key for a specific fragment name."""
    if vary_on:
        fragment_name = "%s_" % fragment_name
    return TEMPLATE_FRAGMENT_KEY_TEMPLATE % (fragment_name, "_".join(vary_on))


class Cache(object):
    """This class is used to control the cache objects."""

    def __init__(self, app=None, with_jinja2_ext=True, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        self.with_jinja2_ext = with_jinja2_ext
        self.config = config

        if app is not None:
            self.init_app(app, config)

    def init_app(self, app, config=None):
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

        if (
            config["CACHE_TYPE"] == "null"
            and not config["CACHE_NO_NULL_WARNING"]
        ):
            warnings.warn(
                "Flask-Caching: CACHE_TYPE is set to null, "
                "caching is effectively disabled."
            )

        if self.with_jinja2_ext:
            from .jinja2ext import CacheExtension, JINJA_CACHE_ATTR_NAME

            setattr(app.jinja_env, JINJA_CACHE_ATTR_NAME, self)
            app.jinja_env.add_extension(CacheExtension)

        self._set_cache(app, config)

    def _set_cache(self, app, config):
        import_me = config["CACHE_TYPE"]
        if "." not in import_me:
            from . import backends

            try:
                cache_obj = getattr(backends, import_me)
            except AttributeError:
                raise ImportError(
                    "%s is not a valid Flask-Caching backend" % (import_me)
                )
        else:
            cache_obj = import_string(import_me)

        cache_args = config["CACHE_ARGS"][:]
        cache_options = {"default_timeout": config["CACHE_DEFAULT_TIMEOUT"]}

        if config["CACHE_OPTIONS"]:
            cache_options.update(config["CACHE_OPTIONS"])

        if not hasattr(app, "extensions"):
            app.extensions = {}

        app.extensions.setdefault("cache", {})
        app.extensions["cache"][self] = cache_obj(
            app, config, cache_args, cache_options
        )
        self.app = app

    @property
    def cache(self):
        app = current_app or self.app
        return app.extensions["cache"][self]

    def get(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.get(*args, **kwargs)

    def set(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.set(*args, **kwargs)

    def add(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.add(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.delete(*args, **kwargs)

    def delete_many(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.delete_many(*args, **kwargs)

    def clear(self):
        """Proxy function for internal cache object."""
        return self.cache.clear()

    def get_many(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.get_many(*args, **kwargs)

    def set_many(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.set_many(*args, **kwargs)

    def get_dict(self, *args, **kwargs):
        """Proxy function for internal cache object."""
        return self.cache.get_dict(*args, **kwargs)

    def cached(
        self,
        timeout=None,
        key_prefix="view/%s",
        unless=None,
        forced_update=None,
        response_filter=None,
        query_string=False,
        hash_method=hashlib.md5,
    ):
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
                                invoked after the cached funtion evaluation,
                                and is given one arguement, the response
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

        """

        def decorator(f):
            @functools.wraps(f)
            def decorated_function(*args, **kwargs):
                #: Bypass the cache entirely.
                if self._bypass_cache(unless, f, *args, **kwargs):
                    return f(*args, **kwargs)

                try:
                    if query_string:
                        cache_key = _make_cache_key_query_string()
                    else:
                        cache_key = _make_cache_key(
                            args, kwargs, use_request=True
                        )

                    if callable(forced_update) and forced_update() is True:
                        rv = None
                        found = False
                    else:
                        rv = self.cache.get(cache_key)
                        found = True

                        # If the value returned by cache.get() is None, it
                        # might be because the key is not found in the cache
                        # or because the cached value is actually None
                        if rv is None:
                            found = self.cache.has(cache_key)
                except Exception:
                    if self.app.debug:
                        raise
                    logger.exception(
                        "Exception possibly due to cache backend."
                    )
                    return f(*args, **kwargs)

                if not found:
                    rv = f(*args, **kwargs)

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
                            logger.exception(
                                "Exception possibly due to cache backend."
                            )
                return rv

            def make_cache_key(*args, **kwargs):
                # Convert non-keyword arguments (which is the way
                # `make_cache_key` expects them) to keyword arguments
                # (the way `url_for` expects them)
                try:
                    # Python >= 3.0
                    argspec_args = inspect.getfullargspec(f).args
                except AttributeError:
                    argspec_args = inspect.getargspec(f).args

                for arg_name, arg in zip(argspec_args, args):
                    kwargs[arg_name] = arg

                return _make_cache_key(args, kwargs, use_request=False)

            def _make_cache_key_query_string():
                """Create consistent keys for query string arguments.

                Produces the same cache key regardless of argument order, e.g.,
                both `?limit=10&offset=20` and `?offset=20&limit=10` will
                always produce the same exact cache key.
                """

                # Create a tuple of (key, value) pairs, where the key is the
                # argument name and the value is its respective value. Order
                # this tuple by key. Doing this ensures the cache key created
                # is always the same for query string args whose keys/values
                # are the same, regardless of the order in which they are
                # provided.
                args_as_sorted_tuple = tuple(
                    sorted((pair for pair in request.args.items(multi=True)))
                )
                # ... now hash the sorted (key, value) tuple so it can be
                # used as a key for cache. Turn them into bytes so that the
                # hash function will accept them
                args_as_bytes = str(args_as_sorted_tuple).encode()
                hashed_args = str(hash_method(args_as_bytes).hexdigest())
                cache_key = request.path + hashed_args
                return cache_key

            def _make_cache_key(args, kwargs, use_request):
                if callable(key_prefix):
                    cache_key = key_prefix()
                elif "%s" in key_prefix:
                    if use_request:
                        cache_key = key_prefix % request.path
                    else:
                        cache_key = key_prefix % url_for(f.__name__, **kwargs)
                else:
                    cache_key = key_prefix

                return cache_key

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = make_cache_key

            return decorated_function

        return decorator

    def _memvname(self, funcname):
        return funcname + "_memver"

    def _memoize_make_version_hash(self):
        return base64.b64encode(uuid.uuid4().bytes)[:6].decode("utf-8")

    def _memoize_version(
        self,
        f,
        args=None,
        reset=False,
        delete=False,
        timeout=None,
        forced_update=False,
    ):
        """Updates the hash version associated with a memoized function or
        method.
        """
        fname, instance_fname = function_namespace(f, args=args)
        version_key = self._memvname(fname)
        fetch_keys = [version_key]

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

        if callable(forced_update) and forced_update() is True:
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
        make_name=None,
        timeout=None,
        forced_update=False,
        hash_method=hashlib.md5,
    ):
        """Function used to create the cache_key for memoized functions."""

        def make_cache_key(f, *args, **kwargs):
            _timeout = getattr(timeout, "cache_timeout", timeout)
            fname, version_data = self._memoize_version(
                f, args=args, timeout=_timeout, forced_update=forced_update
            )

            #: this should have to be after version_data, so that it
            #: does not break the delete_memoized functionality.
            altfname = make_name(fname) if callable(make_name) else fname

            if callable(f):
                keyargs, keykwargs = self._memoize_kwargs_to_args(
                    f, *args, **kwargs
                )
            else:
                keyargs, keykwargs = args, kwargs

            updated = u"{0}{1}{2}".format(altfname, keyargs, keykwargs)

            cache_key = hash_method()
            cache_key.update(updated.encode("utf-8"))
            cache_key = base64.b64encode(cache_key.digest())[:16]
            cache_key = cache_key.decode("utf-8")
            cache_key += version_data

            return cache_key

        return make_cache_key

    def _memoize_kwargs_to_args(self, f, *args, **kwargs):
        #: Inspect the arguments to the function
        #: This allows the memoization to be the same
        #: whether the function was called with
        #: 1, b=2 is equivilant to a=1, b=2, etc.
        new_args = []
        arg_num = 0

        # If the function uses VAR_KEYWORD type of parameters,
        # we need to pass these further
        kw_keys_remaining = list(kwargs.keys())
        arg_names = get_arg_names(f)
        args_len = len(arg_names)

        for i in range(args_len):
            arg_default = get_arg_default(f, i)
            if i == 0 and arg_names[i] in ("self", "cls"):
                #: use the repr of the class instance
                #: this supports instance methods for
                #: the memoized functions, giving more
                #: flexibility to developers
                arg = repr(args[0])
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
            #         arg = repr(arg)

            #: Or what about a special __cacherepr__ function
            #: on an object, this allows objects to act normal
            #: upon inspection, yet they can define a representation
            #: that can be used to make the object unique in the
            #: cache key. Given that a case comes across that
            #: an object "must" be used as a cache key
            # if hasattr(arg, '__cacherepr__'):
            #     arg = arg.__cacherepr__

            new_args.append(arg)

        new_args.extend(args[len(arg_names):])
        return (
            tuple(new_args),
            OrderedDict(
                sorted(
                    (k, v)
                    for k, v in iteritems(kwargs)
                    if k in kw_keys_remaining
                )
            ),
        )

    def _bypass_cache(self, unless, f, *args, **kwargs):
        """Determines whether or not to bypass the cache by calling unless().
        Supports both unless() that takes in arguments and unless()
        that doesn't.
        """
        bypass_cache = False

        if callable(unless):
            try:
                # Python >= 3.0
                argspec = inspect.getfullargspec(unless)
                has_args = (
                    len(argspec.args) > 0 or
                    argspec.varargs or
                    argspec.varkw
                )
            except AttributeError:
                argspec = inspect.getargspec(unless)
                has_args = (
                    len(argspec.args) > 0 or
                    argspec.varargs or
                    argspec.keywords
                )

            # If unless() takes args, pass them in.
            if has_args:
                if unless(f, *args, **kwargs) is True:
                    bypass_cache = True
            elif unless() is True:
                bypass_cache = True

        return bypass_cache

    def memoize(
        self,
        timeout=None,
        make_name=None,
        unless=None,
        forced_update=None,
        hash_method=hashlib.md5,
    ):
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
        :param hash_method: Default hashlib.md5. The hash method used to
                            generate the keys for cached results.

        .. versionadded:: 0.5
            params ``make_name``, ``unless``
        """

        def memoize(f):
            @functools.wraps(f)
            def decorated_function(*args, **kwargs):
                #: bypass cache
                if self._bypass_cache(unless, f, *args, **kwargs):
                    return f(*args, **kwargs)

                try:
                    cache_key = decorated_function.make_cache_key(
                        f, *args, **kwargs
                    )

                    if callable(forced_update) and forced_update() is True:
                        rv = None
                        found = False
                    else:
                        rv = self.cache.get(cache_key)
                        found = True

                        # If the value returned by cache.get() is None, it
                        # might be because the key is not found in the cache
                        # or because the cached value is actually None
                        if rv is None:
                            found = self.cache.has(cache_key)
                except Exception:
                    if self.app.debug:
                        raise
                    logger.exception(
                        "Exception possibly due to cache backend."
                    )
                    return f(*args, **kwargs)

                if not found:
                    rv = f(*args, **kwargs)
                    try:
                        self.cache.set(
                            cache_key,
                            rv,
                            timeout=decorated_function.cache_timeout,
                        )
                    except Exception:
                        if self.app.debug:
                            raise
                        logger.exception(
                            "Exception possibly due to cache backend."
                        )
                return rv

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = self._memoize_make_cache_key(
                make_name=make_name,
                timeout=decorated_function,
                forced_update=forced_update,
                hash_method=hash_method,
            )
            decorated_function.delete_memoized = lambda: self.delete_memoized(f)

            return decorated_function

        return memoize

    def delete_memoized(self, f, *args, **kwargs):
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
        :param \*args: A list of positional parameters used with
                       memoized function.
        :param \**kwargs: A dict of named parameters used with
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

    def delete_memoized_verhash(self, f, *args):
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
