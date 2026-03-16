"""
Python dependency injection framework.

Usage:
- Create an optional configuration::
    def my_config(binder):
        binder.bind(Cache, RedisCache('localhost:1234'))
        binder.bind_to_provider(CurrentUser, get_current_user)

- Create a shared injector::
    inject.configure(my_config)

- Use `inject.instance`, `inject.attr` or `inject.param` to inject dependencies::
    class User(object):
        cache = inject.attr(Cache)

        @classmethod
        def load(cls, id):
            return cls.cache.load('user', id)

        def save(self):
            self.cache.save(self)

    def foo(bar):
        cache = inject.instance(Cache)
        cache.save('bar', bar)

    @inject.params(cache=Cache)
    def bar(foo, cache=None):
        cache.save('foo', foo)

Binding types:
- Instance bindings configured via `bind(cls, instance) which always return the same instance.
- Constructor bindings `bind_to_constructor(cls, callable)` which create a singleton
  on first access.
- Provider bindings `bind_to_provider(cls, callable)` which call the provider
  for each injection.
- Runtime bindings which automatically create class singletons.

Thread-safety:
After configuration the injector is thread-safe and can be safely reused by multiple threads.

Unit testing:
In tests use `inject.clear_and_configure(callable)` to create a new injector on setup,
and `inject.clear()` to clean-up on tear down.

Runtime bindings greatly reduce the required configuration by automatically creating singletons
on first access. For example, below only the Config class requires binding configuration,
all other classes are runtime bindings::
    class Cache(object):
        config = inject.attr(Config)

        def __init__(self):
            self._redis = connect(self.config.redis_address)

    class Db(object):
        pass

    class UserRepo(object):
        cache = inject.attr(Cache)
        db = inject.attr(Db)

        def load(self, user_id):
            return cache.load('user', user_id) or db.load('user', user_id)

    class Config(object):
        def __init__(self, redis_address):
            self.redis_address = redis_address

    def my_config(binder):
        binder.bind(Config, load_config_file())

    inject.configure(my_config)

"""
__version__ = '3.5.1dev0'
__author__ = 'Ivan Korobkov <ivan.korobkov@gmail.com>'
__license__ = 'Apache License 2.0'
__url__ = 'https://github.com/ivan-korobkov/python-inject'

from functools import wraps
import inspect
import logging
import sys
import threading
from typing import Optional, Type, Hashable, Callable, TypeVar, Union

logger = logging.getLogger('inject')

_INJECTOR = None  # Shared injector instance.
_INJECTOR_LOCK = threading.RLock()  # Guards injector initialization.
_BINDING_LOCK = threading.RLock()  # Guards runtime bindings.

T = TypeVar('T')
Binding = Union[Type[T], Hashable]
Constructor = Provider = Callable[[], T]
BinderCallable = Callable[['Binder'], None]


def configure(config=None, bind_in_runtime=True):
    # type: (Optional[BinderCallable], bool) -> Injector
    """Create an injector with a callable config or raise an exception when already configured."""
    global _INJECTOR

    with _INJECTOR_LOCK:
        if _INJECTOR:
            raise InjectorException('Injector is already configured')

        _INJECTOR = Injector(config, bind_in_runtime=bind_in_runtime)
        logger.debug('Created and configured an injector, config=%s', config)
        return _INJECTOR


def configure_once(config=None, bind_in_runtime=True):
    # type: (Optional[BinderCallable], bool) -> Injector
    """Create an injector with a callable config if not present, otherwise, do nothing."""
    with _INJECTOR_LOCK:
        if _INJECTOR:
            return _INJECTOR

        return configure(config, bind_in_runtime=bind_in_runtime)


def clear_and_configure(config=None, bind_in_runtime=True):
    # type: (Optional[BinderCallable], bool) -> Injector
    """Clear an existing injector and create another one with a callable config."""
    with _INJECTOR_LOCK:
        clear()
        return configure(config, bind_in_runtime=bind_in_runtime)


def is_configured():
    # type: () -> bool
    """Return true if an injector is already configured."""
    with _INJECTOR_LOCK:
        return _INJECTOR is not None


def clear():
    # type: () -> None
    """Clear an existing injector if present."""
    global _INJECTOR

    with _INJECTOR_LOCK:
        if _INJECTOR is None:
            return

        _INJECTOR = None
        logger.debug('Cleared an injector')


def instance(cls):
    # type: (Binding) -> T
    """Inject an instance of a class."""
    return get_injector_or_die().get_instance(cls)


def attr(cls):
    # type: (Binding) -> T
    """Return a attribute injection (descriptor)."""
    return _AttributeInjection(cls)


def param(name, cls=None):
    # type: (str, Binding) -> Callable
    """Deprecated, use @inject.params. Return a decorator which injects an arg into a function."""
    return _ParameterInjection(name, cls)


def params(**args_to_classes):
    # type: (Binding) -> Callable
    """Return a decorator which injects args into a function.

    For example::

        @inject.params(cache=RedisCache, db=DbInterface)
        def sign_up(name, email, cache, db):
            pass
    """
    return _ParametersInjection(**args_to_classes)


def autoparams(*selected_args):
    # type: (str) -> Callable
    """Return a decorator that will inject args into a function using type annotations, Python >= 3.5 only.

    For example::

        @inject.autoparams()
        def refresh_cache(cache: RedisCache, db: DbInterface):
            pass

    There is an option to specify which arguments we want to inject without attempts of injecting everything:

    For example::

        @inject.autoparams('cache', 'db')
        def sign_up(name, email, cache, db):
            pass
    """
    def autoparams_decorator(func):
        if sys.version_info[:2] < (3, 5):
            raise InjectorException('autoparams are supported from Python 3.5 onwards')

        full_args_spec = inspect.getfullargspec(func)
        annotations_items = full_args_spec.annotations.items()
        all_arg_names = frozenset(full_args_spec.args + full_args_spec.kwonlyargs)
        args_to_check = frozenset(selected_args) or all_arg_names
        args_annotated_types = {
            arg_name: annotated_type for arg_name, annotated_type in annotations_items
            if arg_name in args_to_check
        }
        return _ParametersInjection(**args_annotated_types)(func)

    return autoparams_decorator


def get_injector():
    # type: () -> Injector
    """Return the current injector or None."""
    return _INJECTOR


def get_injector_or_die():
    # type: () -> Injector
    """Return the current injector or raise an InjectorException."""
    injector = _INJECTOR
    if not injector:
        raise InjectorException('No injector is configured')

    return injector


class Binder(object):
    def __init__(self):
        self._bindings = {}

    def install(self, config):
        # type: (BinderCallable) -> Binder
        """Install another callable configuration."""
        config(self)
        return self

    def bind(self, cls, instance):
        # type: (Binding, T) -> Binder
        """Bind a class to an instance."""
        self._check_class(cls)
        self._bindings[cls] = lambda: instance
        logger.debug('Bound %s to an instance %s', cls, instance)
        return self

    def bind_to_constructor(self, cls, constructor):
        # type: (Binding, Constructor) -> Binder
        """Bind a class to a callable singleton constructor."""
        self._check_class(cls)
        if constructor is None:
            raise InjectorException('Constructor cannot be None, key=%s' % cls)

        self._bindings[cls] = _ConstructorBinding(constructor)
        logger.debug('Bound %s to a constructor %s', cls, constructor)
        return self

    def bind_to_provider(self, cls, provider):
        # type: (Binding, Provider) -> Binder
        """Bind a class to a callable instance provider executed for each injection."""
        self._check_class(cls)
        if provider is None:
            raise InjectorException('Provider cannot be None, key=%s' % cls)

        self._bindings[cls] = provider
        logger.debug('Bound %s to a provider %s', cls, provider)
        return self

    def _check_class(self, cls):
        # type: (Binding) -> None
        if cls is None:
            raise InjectorException('Binding key cannot be None')

        if cls in self._bindings:
            raise InjectorException('Duplicate binding, key=%s' % cls)


class Injector(object):
    def __init__(self, config=None, bind_in_runtime=True):
        self._bind_in_runtime = bind_in_runtime
        if config:
            binder = Binder()
            config(binder)
            self._bindings = dict(binder._bindings)
        else:
            self._bindings = {}

    def get_instance(self, cls):
        # type: (Binding) -> T
        """Return an instance for a class."""
        binding = self._bindings.get(cls)
        if binding:
            return binding()

        # Try to create a runtime binding.
        with _BINDING_LOCK:
            binding = self._bindings.get(cls)
            if binding:
                return binding()

            if not self._bind_in_runtime:
                raise InjectorException('No binding was found for key=%s' % cls)

            if not callable(cls):
                raise InjectorException(
                    'Cannot create a runtime binding, the key is not callable, key=%s' % cls)

            instance = cls()
            self._bindings[cls] = lambda: instance

            logger.debug('Created a runtime binding for key=%s, instance=%s', cls, instance)
            return instance


class InjectorException(Exception):
    pass


class _ConstructorBinding(object):
    def __init__(self, constructor):
        self._constructor = constructor
        self._created = False
        self._instance = None

    def __call__(self):
        if self._created:
            return self._instance

        with _BINDING_LOCK:
            if self._created:
                return self._instance

            self._instance = self._constructor()
            self._created = True

        return self._instance


class _AttributeInjection(object):
    def __init__(self, cls):
        self._cls = cls

    def __get__(self, obj, owner):
        return instance(self._cls)


class _ParameterInjection(object):
    __slots__ = ('_name', '_cls')

    def __init__(self, name, cls=None):
        self._name = name
        self._cls = cls

    def __call__(self, func):
        @wraps(func)
        def injection_wrapper(*args, **kwargs):
            if self._name not in kwargs:
                kwargs[self._name] = instance(self._cls or self._name)
            return func(*args, **kwargs)

        return injection_wrapper


class _ParametersInjection(object):
    __slots__ = ('_params', )

    def __init__(self, **kwargs):
        self._params = kwargs

    def __call__(self, func):
        if sys.version_info.major == 2:
            arg_names = inspect.getargspec(func).args
        else:
            arg_names = inspect.getfullargspec(func).args
        params_to_provide = self._params

        @wraps(func)
        def injection_wrapper(*args, **kwargs):

            provided_params = frozenset(arg_names[:len(args)]) | frozenset(kwargs.keys())
            for param, cls in params_to_provide.items():
                if param not in provided_params:
                    kwargs[param] = instance(cls)

            return func(*args, **kwargs)

        return injection_wrapper
