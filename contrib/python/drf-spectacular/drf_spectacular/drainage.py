import contextlib
import functools
import inspect
import sys
from collections import defaultdict
from typing import Any, Callable, DefaultDict, List, Optional, Tuple, TypeVar

if sys.version_info >= (3, 8):
    from typing import (  # type: ignore[attr-defined] # noqa: F401
        Final, Literal, TypedDict, _TypedDictMeta,
    )
else:
    from typing_extensions import Final, Literal, TypedDict, _TypedDictMeta  # noqa: F401

if sys.version_info >= (3, 10):
    from typing import TypeGuard  # noqa: F401
else:
    from typing_extensions import TypeGuard  # noqa: F401

F = TypeVar('F', bound=Callable[..., Any])


class GeneratorStats:
    _warn_cache: DefaultDict[str, int] = defaultdict(int)
    _error_cache: DefaultDict[str, int] = defaultdict(int)
    _traces: List[Tuple[Optional[str], Optional[str], str]] = []
    _trace_lineno = False

    _blue = ''
    _red = ''
    _yellow = ''
    _clear = ''

    def __getattr__(self, name):
        if 'silent' not in self.__dict__:
            from drf_spectacular.settings import spectacular_settings
            self.silent = spectacular_settings.DISABLE_ERRORS_AND_WARNINGS
        try:
            return self.__dict__[name]
        except KeyError:
            raise AttributeError(name)

    def __bool__(self):
        return bool(self._warn_cache or self._error_cache)

    @contextlib.contextmanager
    def silence(self):
        self.silent, tmp = True, self.silent
        try:
            yield
        finally:
            self.silent = tmp

    def reset(self) -> None:
        self._warn_cache.clear()
        self._error_cache.clear()

    def enable_color(self) -> None:
        self._blue = '\033[0;34m'
        self._red = '\033[0;31m'
        self._yellow = '\033[0;33m'
        self._clear = '\033[0m'

    def enable_trace_lineno(self) -> None:
        self._trace_lineno = True

    def _get_current_trace(self) -> Tuple[Optional[str], str]:
        source_locations = [t for t in self._traces if t[0]]
        if source_locations:
            sourcefile, lineno, _ = source_locations[-1]
            source_location = f'{sourcefile}:{lineno}' if lineno else sourcefile
        else:
            source_location = ''
        breadcrumbs = ' > '.join(t[2] for t in self._traces)
        return source_location, breadcrumbs

    def emit(self, msg: str, severity: str) -> None:
        assert severity in ['warning', 'error']
        cache = self._warn_cache if severity == 'warning' else self._error_cache

        source_location, breadcrumbs = self._get_current_trace()
        prefix = f'{self._blue}{source_location}: ' if source_location else ''
        prefix += self._yellow if severity == 'warning' else self._red
        prefix += f'{severity.capitalize()}'
        prefix += f' [{breadcrumbs}]: ' if breadcrumbs else ': '

        msg = prefix + self._clear + str(msg)
        if not self.silent and msg not in cache:
            print(msg, file=sys.stderr)
        cache[msg] += 1

    def emit_summary(self) -> None:
        if not self.silent and (self._warn_cache or self._error_cache):
            print(
                f'\nSchema generation summary:\n'
                f'Warnings: {sum(self._warn_cache.values())} ({len(self._warn_cache)} unique)\n'
                f'Errors:   {sum(self._error_cache.values())} ({len(self._error_cache)} unique)\n',
                file=sys.stderr
            )


GENERATOR_STATS = GeneratorStats()


def warn(msg: str, delayed: Any = None) -> None:
    if delayed:
        warnings = get_override(delayed, 'warnings', [])
        warnings.append(msg)
        set_override(delayed, 'warnings', warnings)
    else:
        GENERATOR_STATS.emit(msg, 'warning')


def error(msg: str, delayed: Any = None) -> None:
    if delayed:
        errors = get_override(delayed, 'errors', [])
        errors.append(msg)
        set_override(delayed, 'errors', errors)
    else:
        GENERATOR_STATS.emit(msg, 'error')


def reset_generator_stats() -> None:
    GENERATOR_STATS.reset()


@contextlib.contextmanager
def add_trace_message(obj):
    """
    Adds a message to be used as a prefix when emitting warnings and errors.
    """
    sourcefile, lineno = _get_source_location(obj)
    GENERATOR_STATS._traces.append((sourcefile, lineno, obj.__name__))
    yield
    GENERATOR_STATS._traces.pop()


@functools.lru_cache(maxsize=1000)
def _get_source_location(obj):
    try:
        sourcefile = inspect.getsourcefile(obj)
    except:  # noqa: E722
        sourcefile = None
    try:
        # This is a rather expensive operation. Only do it when explicitly enabled (CLI)
        # and cache results to speed up some recurring objects like serializers.
        lineno = inspect.getsourcelines(obj)[1] if GENERATOR_STATS._trace_lineno else None
    except:  # noqa: E722
        lineno = None
    return sourcefile, lineno


def has_override(obj: Any, prop: str) -> bool:
    if isinstance(obj, functools.partial):
        obj = obj.func
    if not hasattr(obj, '_spectacular_annotation'):
        return False
    if prop not in obj._spectacular_annotation:
        return False
    return True


def get_override(obj: Any, prop: str, default: Any = None) -> Any:
    if isinstance(obj, functools.partial):
        obj = obj.func
    if not has_override(obj, prop):
        return default
    return obj._spectacular_annotation[prop]


def set_override(obj: Any, prop: str, value: Any) -> Any:
    if not hasattr(obj, '_spectacular_annotation'):
        obj._spectacular_annotation = {}
    elif '_spectacular_annotation' not in obj.__dict__:
        obj._spectacular_annotation = obj._spectacular_annotation.copy()
    obj._spectacular_annotation[prop] = value
    return obj


def get_view_method_names(view, schema=None) -> List[str]:
    schema = schema or view.schema
    return [
        item for item in dir(view) if callable(getattr(view, item, None)) and (
            item in view.http_method_names
            or item in schema.method_mapping.values()
            or item == 'list'
            or hasattr(getattr(view, item, None), 'mapping')
        )
    ]


def isolate_view_method(view, method_name):
    """
    Prevent modifying a view method which is derived from other views. Changes to
    a derived method would leak into the view where the method originated from.
    Break derivation by wrapping the method and explicitly setting it on the view.
    """
    method = getattr(view, method_name)
    # no isolation is required if the view method is not derived.
    # @api_view is a special case that also breaks isolation. It proxies all view
    # methods through a single handler function, which then also requires isolation.
    if method_name in view.__dict__ and method.__name__ != 'handler':
        return method

    @functools.wraps(method)
    def wrapped_method(self, request, *args, **kwargs):
        return method(self, request, *args, **kwargs)

    # wraps() will only create a shallow copy of method.__dict__. Updates to "kwargs"
    # via @extend_schema would leak to the original method. Isolate by creating a copy.
    if hasattr(method, 'kwargs'):
        wrapped_method.kwargs = method.kwargs.copy()

    setattr(view, method_name, wrapped_method)
    return wrapped_method


def cache(user_function: F) -> F:
    """ simple polyfill for python < 3.9 """
    return functools.lru_cache(maxsize=None)(user_function)  # type: ignore
