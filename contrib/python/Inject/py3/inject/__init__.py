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
import contextlib

from inject._version import __version__

import inspect
import logging
import sys
import threading
from functools import wraps
from typing import (Any, Awaitable, Callable, Dict, Generic, Hashable,
                    Optional, Set, Type, TypeVar, Union, cast, get_type_hints,
                    overload)

_HAS_PEP604_SUPPORT = sys.version_info[:3] >= (3, 10, 0)  # PEP 604
if _HAS_PEP604_SUPPORT:
    _HAS_PEP560_SUPPORT = True
else:
    _HAS_PEP560_SUPPORT = sys.version_info[:3] >= (3, 7, 0)  # PEP 560
_RETURN = 'return'

if _HAS_PEP604_SUPPORT:
    from types import UnionType
    from typing import ForwardRef, _GenericAlias
elif _HAS_PEP560_SUPPORT:
    from typing import ForwardRef, _GenericAlias
else:
    from typing import _Union


logger = logging.getLogger('inject')

_INJECTOR = None  # Shared injector instance.
_INJECTOR_LOCK = threading.RLock()  # Guards injector initialization.
_BINDING_LOCK = threading.RLock()  # Guards runtime bindings.

Injectable = Union[object, Any]
T = TypeVar('T', bound=Injectable)
Binding = Union[Type[Injectable], Hashable]
Constructor = Callable[[], Injectable]
Provider = Constructor
BinderCallable = Callable[['Binder'], Optional['Binder']]


class ConstructorTypeError(TypeError):
    def __init__(self, constructor: Callable, previous_error: TypeError):
        super(ConstructorTypeError, self).__init__("%s raised an error: %s" % (constructor, previous_error))


class Binder(object):
    _bindings: Dict[Binding, Constructor]

    def __init__(self, allow_override: bool = False) -> None:
        self._bindings = {}
        self.allow_override = allow_override

    def install(self, config: BinderCallable) -> 'Binder':
        """Install another callable configuration."""
        config(self)
        return self

    def bind(self, cls: Binding, instance: T) -> 'Binder':
        """Bind a class to an instance."""
        self._check_class(cls)

        b = lambda: instance
        self._bindings[cls] = b
        self._maybe_bind_forward(cls, b)

        logger.debug('Bound %s to an instance %s', cls, instance)
        return self

    def bind_to_constructor(self, cls: Binding, constructor: Constructor) -> 'Binder':
        """Bind a class to a callable singleton constructor."""
        self._check_class(cls)
        if constructor is None:
            raise InjectorException('Constructor cannot be None, key=%s' % cls)
        
        b = _ConstructorBinding(constructor)
        self._bindings[cls] = b
        self._maybe_bind_forward(cls, b)

        logger.debug('Bound %s to a constructor %s', cls, constructor)
        return self

    def bind_to_provider(self, cls: Binding, provider: Provider) -> 'Binder':
        """
        Bind a class to a callable instance provider executed for each injection.
        A provider can be a normal function or a context manager. Both sync and async are supported.
        """
        self._check_class(cls)
        if provider is None:
            raise InjectorException('Provider cannot be None, key=%s' % cls)

        b = provider
        self._bindings[cls] = b
        self._maybe_bind_forward(cls, b)

        logger.debug('Bound %s to a provider %s', cls, provider)
        return self

    def _check_class(self, cls: Binding) -> None:
        if cls is None:
            raise InjectorException('Binding key cannot be None')

        if not self.allow_override and cls in self._bindings:
            raise InjectorException('Duplicate binding, key=%s' % cls)

        if self._is_forward_str(cls):
            ref = ForwardRef(cls)
            if ref in self._bindings:
                raise InjectorException('Duplicate forward binding, i.e. "int" and int, key=%s', cls)
    
    def _maybe_bind_forward(self, cls: Binding, binding: Any) -> None:
        """Bind a string forward reference."""
        if not _HAS_PEP560_SUPPORT:
            return
        if not isinstance(cls, str):
            return
        
        ref = ForwardRef(cls)
        self._bindings[ref] = binding
        logger.debug('Bound forward ref "%s"', cls)

    def _is_forward_str(self, cls: Binding) -> bool:
        return _HAS_PEP560_SUPPORT and isinstance(cls, str)


class Injector(object):
    _bindings: Dict[Binding, Constructor]

    def __init__(
        self, config: Optional[BinderCallable] = None, bind_in_runtime: bool = True, allow_override: bool = False
    ):
        self._bind_in_runtime = bind_in_runtime
        if config:
            binder = Binder(allow_override)
            config(binder)
            self._bindings = binder._bindings
        else:
            self._bindings = {}

    @overload
    def get_instance(self, cls: Type[T]) -> T: ...

    @overload
    def get_instance(self, cls: Hashable) -> Injectable: ...

    def get_instance(self, cls: Binding) -> Injectable:
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
                raise InjectorException(
                    'No binding was found for key=%s' % cls)

            if not callable(cls):
                raise InjectorException(
                    'Cannot create a runtime binding, the key is not callable, key=%s' % cls)

            try:
                instance = cls()
            except TypeError as previous_error:
                raise ConstructorTypeError(cls, previous_error)

            self._bindings[cls] = lambda: instance

            logger.debug(
                'Created a runtime binding for key=%s, instance=%s', cls, instance)
            return instance


class InjectorException(Exception):
    pass


class _ConstructorBinding(Generic[T]):
    _instance: Optional[T]

    def __init__(self, constructor: Callable[[], T]) -> None:
        self._constructor = constructor
        self._created = False
        self._instance = None

    def __call__(self) -> T:
        if self._created and self._instance is not None:
            return self._instance

        with _BINDING_LOCK:
            if self._created and self._instance is not None:
                return self._instance
            self._instance = self._constructor()
            self._created = True
        return self._instance


class _AttributeInjection(object):
    def __init__(self, cls: Binding) -> None:
        self._cls = cls

    def __get__(self, obj: Any, owner: Any) -> Injectable:
        inst = instance(self._cls)
        if isinstance(inst, contextlib._AsyncGeneratorContextManager):
            raise InjectorException(
                    'Fail to load _AsyncGeneratorContextManager, use autoparams, param or params instead of attr function')
        elif isinstance(inst, contextlib._GeneratorContextManager):
            with contextlib.ExitStack() as sync_stack:
                inst = sync_stack.enter_context(inst)
        return inst


class _AttributeInjectionDataclass(Generic[T]):
    def __init__(self, cls: Binding) -> None:
        self._cls = cls

    def __get__(self, instance, owner) -> T:
        injector = get_injector()
        if injector is not None:
            return injector.get_instance(self._cls)
        raise AttributeError


class _ParameterInjection(Generic[T]):
    __slots__ = ('_name', '_cls')

    def __init__(self, name: str, cls: Optional[Binding] = None) -> None:
        self._name = name
        self._cls = cls

    def __call__(self, func: Callable[..., Union[T, Awaitable[T]]]) -> Callable[..., Union[T, Awaitable[T]]]:
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_injection_wrapper(*args: Any, **kwargs: Any) -> T:
                if self._name not in kwargs:
                    kwargs[self._name] = instance(self._cls or self._name)
                async_func = cast(Callable[..., Awaitable[T]], func)
                return await async_func(*args, **kwargs)
            return async_injection_wrapper
        
        @wraps(func)
        def injection_wrapper(*args: Any, **kwargs: Any) -> T:
            if self._name not in kwargs:
                kwargs[self._name] = instance(self._cls or self._name)
            sync_func = cast(Callable[..., T], func)
            return sync_func(*args, **kwargs)

        return injection_wrapper


class _ParametersInjection(Generic[T]):
    __slots__ = ('_params', )

    def __init__(self, **kwargs: Any) -> None:
        self._params = kwargs

    @staticmethod
    def _aggregate_sync_stack(
            sync_stack: contextlib.ExitStack,
            provided_params: frozenset[str],
            kwargs: dict[str, Any]
    ) -> None:
        """Extracts context managers, aggregate them in an ExitStack and swap out the param value with results of
        running __enter__(). The result is equivalent to using `with` multiple times """
        executed_kwargs = {
            param: sync_stack.enter_context(inst)
            for param, inst in kwargs.items()
            if param not in provided_params and isinstance(inst, contextlib._GeneratorContextManager)
        }
        kwargs.update(executed_kwargs)

    @staticmethod
    async def _aggregate_async_stack(
            async_stack: contextlib.AsyncExitStack,
            provided_params: frozenset[str],
            kwargs: dict[str, Any]
    ) -> None:
        """Similar to _aggregate_sync_stack, but for async context managers"""
        executed_kwargs = {
            param: await async_stack.enter_async_context(inst)
            for param, inst in kwargs.items()
            if param not in provided_params and isinstance(inst, contextlib._AsyncGeneratorContextManager)
        }
        kwargs.update(executed_kwargs)

    def __call__(self, func: Callable[..., Union[Awaitable[T], T]]) -> Callable[..., Union[Awaitable[T], T]]:
        if sys.version_info.major == 2:
            arg_names = inspect.getargspec(func).args
        else:
            arg_names = inspect.getfullargspec(func).args
        params_to_provide = self._params

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_injection_wrapper(*args: Any, **kwargs: Any) -> T:
                provided_params = frozenset(
                    arg_names[:len(args)]) | frozenset(kwargs.keys())
                for param, cls in params_to_provide.items():
                    if param not in provided_params:
                        kwargs[param] = instance(cls)
                async_func = cast(Callable[..., Awaitable[T]], func)
                try:
                    with contextlib.ExitStack() as sync_stack:
                        async with contextlib.AsyncExitStack() as async_stack:
                            self._aggregate_sync_stack(sync_stack, provided_params, kwargs)
                            await self._aggregate_async_stack(async_stack, provided_params, kwargs)
                            return await async_func(*args, **kwargs)
                except TypeError as previous_error:
                    raise ConstructorTypeError(func, previous_error)

            return async_injection_wrapper

        @wraps(func)
        def injection_wrapper(*args: Any, **kwargs: Any) -> T:
            provided_params = frozenset(
                arg_names[:len(args)]) | frozenset(kwargs.keys())
            for param, cls in params_to_provide.items():
                if param not in provided_params:
                    kwargs[param] = instance(cls)
            sync_func = cast(Callable[..., T], func)
            try:
                with contextlib.ExitStack() as sync_stack:
                    self._aggregate_sync_stack(sync_stack, provided_params, kwargs)
                    return sync_func(*args, **kwargs)
            except TypeError as previous_error:
                raise ConstructorTypeError(func, previous_error)
        return injection_wrapper


def configure(
    config: Optional[BinderCallable] = None, 
    bind_in_runtime: bool = True, 
    allow_override: bool = False,
    clear: bool = False,
    once: bool = False
) -> Injector:
    """Create an injector with a callable config or raise an exception when already configured."""
    global _INJECTOR

    if clear and once:
        raise InjectorException('clear and once are mutually exclusive, only one can be True')

    with _INJECTOR_LOCK:
        if _INJECTOR:
            if clear:
                _clear_injector()
            elif once:
                return _INJECTOR
            else:
                raise InjectorException('Injector is already configured')

        _INJECTOR = Injector(config, bind_in_runtime=bind_in_runtime, allow_override=allow_override)
        logger.debug('Created and configured an injector, config=%s', config)
        return _INJECTOR


def configure_once(
    config: Optional[BinderCallable] = None, 
    bind_in_runtime: bool = True, 
    allow_override: bool = False
) -> Injector:
    """Create an injector with a callable config if not present, otherwise, do nothing.
    
    Deprecated, use `configure(once=True)` instead.
    """
    with _INJECTOR_LOCK:
        if _INJECTOR:
            return _INJECTOR

        return configure(config, bind_in_runtime=bind_in_runtime, allow_override=allow_override)


def clear_and_configure(
    config: Optional[BinderCallable] = None, 
    bind_in_runtime: bool = True, 
    allow_override: bool = False
) -> Injector:
    """Clear an existing injector and create another one with a callable config.
    
    Deprecated, use configure(clear=True) instead.
    """
    with _INJECTOR_LOCK:
        _clear_injector()
        return configure(config, bind_in_runtime=bind_in_runtime, allow_override=allow_override)


def is_configured() -> bool:
    """Return true if an injector is already configured."""
    with _INJECTOR_LOCK:
        return _INJECTOR is not None


def clear() -> None:
    """Clear an existing injector if present."""
    _clear_injector()


def _clear_injector() -> None:
    """Clear an existing injector if present."""
    global _INJECTOR

    with _INJECTOR_LOCK:
        if _INJECTOR is None:
            return

        _INJECTOR = None
        logger.debug('Cleared an injector')


@overload
def instance(cls: Type[T]) -> T: ...

@overload
def instance(cls: Hashable) -> Injectable: ...

def instance(cls: Binding) -> Injectable:
    """Inject an instance of a class."""
    return get_injector_or_die().get_instance(cls)

@overload
def attr(cls: Type[T]) -> T: ...

@overload
def attr(cls: Hashable) -> Injectable: ...

def attr(cls: Binding) -> Injectable:
    """Return a attribute injection (descriptor)."""
    return _AttributeInjection(cls)

@overload
def attr_dc(cls: Type[T]) -> T: ...

@overload
def attr_dc(cls: Hashable) -> Injectable: ...

def attr_dc(cls: Binding) -> Injectable:
    """Return a attribute injection (descriptor)."""
    return _AttributeInjectionDataclass(cls)


def param(name: str, cls: Optional[Binding] = None) -> Callable:
    """Deprecated, use @inject.params. Return a decorator which injects an arg into a function."""
    return _ParameterInjection(name, cls)


def params(**args_to_classes: Binding) -> Callable:
    """Return a decorator which injects args into a function.

    For example::

        @inject.params(cache=RedisCache, db=DbInterface)
        def sign_up(name, email, cache, db):
            pass
    """
    return _ParametersInjection(**args_to_classes)


def autoparams(*selected: str) -> Callable:
    """Return a decorator that will inject args into a function using type annotations, Python >= 3.5 only.

    For example::

        @inject.autoparams
        def refresh_cache(cache: RedisCache, db: DbInterface):
            pass

    There is an option to specify which arguments we want to inject without attempts of injecting everything:

    For example::

        @inject.autoparams('cache', 'db')
        def sign_up(name, email, cache: RedisCache, db: DbInterface):
            pass
    """
    only_these: Set[str] = set()

    def autoparams_decorator(fn: Callable[..., T]) -> Callable[..., T]:
        if inspect.isclass(fn):
            types = get_type_hints(fn.__init__)
        else:
            types = get_type_hints(fn)

        # Skip the return annotation.
        types = {name: typ for name, typ in types.items() if name != _RETURN}

        # Convert Union types into single types, i.e. Union[A, None] => A.
        types = {name: _unwrap_union_arg(typ) for name, typ in types.items()}

        # Filter types if selected args present.
        if only_these:
            types = {name: typ for name, typ in types.items() if name in only_these}
        
        wrapper: _ParametersInjection[T] = _ParametersInjection(**types)
        return wrapper(fn)

    target = selected[0] if selected else None
    if len(selected) == 1 and callable(target):
        return autoparams_decorator(target)

    only_these.update(selected)
    return autoparams_decorator


def get_injector() -> Optional[Injector]:
    """Return the current injector or None."""
    return _INJECTOR


def get_injector_or_die() -> Injector:
    """Return the current injector or raise an InjectorException."""
    injector = _INJECTOR
    if not injector:
        raise InjectorException('No injector is configured')

    return injector


def _unwrap_union_arg(typ):
    """Return the first type A in typing.Union[A, B] or typ if not Union."""
    if not _is_union_type(typ):
        return typ
    return typ.__args__[0]


def _is_union_type(typ):
    """Test if the type is a union type. Examples::
        is_union_type(int) == False
        is_union_type(Union) == True
        is_union_type(Union[int, int]) == False
        is_union_type(Union[T, int]) == True

    Source: https://github.com/ilevkivskyi/typing_inspect/blob/master/typing_inspect.py
    """
    if _HAS_PEP604_SUPPORT:
        return (typ is Union or
                isinstance(typ, UnionType) or
                isinstance(typ, _GenericAlias) and typ.__origin__ is Union)
    elif _HAS_PEP560_SUPPORT:
        return (typ is Union or
                isinstance(typ, _GenericAlias) and typ.__origin__ is Union)
    return type(typ) is _Union
