import contextlib
import inspect
from abc import ABC, abstractmethod
from functools import wraps
from typing import Optional, Type, Union

from annet.connectors import CachedConnector


MinDurationT = Optional[Union[str, int]]


class _Nop:
    def __getattr__(self, item):
        return _Nop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __call__(self, *args, **kwargs):
        return _Nop()

    def __bool__(self):
        return False


class Tracing(ABC):
    enabled = False

    @abstractmethod
    def function(self, arg=None, /, **kwargs):
        pass

    @abstractmethod
    def contextmanager(self, arg=None, /, **kwargs):
        pass

    @abstractmethod
    def get_current_span(self, context_value=None):
        pass

    @abstractmethod
    def set_device_attributes(self, span, device):
        pass

    @abstractmethod
    def start_as_current_span(self, *args, **kwargs):
        pass

    @abstractmethod
    def start_as_linked_span(self, *args, **kwargs):
        pass

    @abstractmethod
    def attach_context(self, *args, **kwargs):
        pass

    @abstractmethod
    def inject_context(self, *args, **kwargs):
        pass

    @abstractmethod
    def extract_context(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_context(self, *args, **kwargs):
        pass

    @abstractmethod
    def force_flush(self):
        pass


class DummyTracing(Tracing):
    def function(self, arg=None, /, **kwargs):
        def decorator(func):
            return func

        return decorator if arg is None else decorator(arg)

    def contextmanager(self, arg=None, /, **kwargs):
        def decorator(cls_or_func):
            return cls_or_func

        return decorator if arg is None else decorator(arg)

    def get_current_span(self, context_value=None):
        return None

    def set_device_attributes(self, span, device):
        return None

    def start_as_current_span(self, *args, **kwargs):
        return _Nop()

    def start_as_linked_span(self, *args, **kwargs):
        return _Nop()

    def attach_context(self, *args, **kwargs):
        return

    def inject_context(self, *args, **kwargs):
        return

    def extract_context(self, *args, **kwargs):
        return

    def get_context(self, *args, **kwargs):
        return

    def force_flush(self):
        return


class _TracingConnector(CachedConnector[Tracing]):
    name = "Tracing"
    ep_name = "tracing"

    def _get_default(self) -> Type[Tracing]:
        return DummyTracing


tracing_connector = _TracingConnector()


def function(arg=None, /, **outer_kwargs):
    def decorator(func):
        cache = None

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal cache
            if cache is None:
                cache = tracing_connector.get().function(func, **outer_kwargs)
            return cache(*args, **kwargs)

        return wrapper

    return decorator if arg is None else decorator(arg)


def contextmanager(arg=None, /, **outer_kwargs):
    def decorator(cls_or_func):
        cache = None

        if inspect.isfunction(cls_or_func):

            @contextlib.contextmanager
            @wraps(cls_or_func)
            def wrapper(*args, **kwargs):
                nonlocal cache
                if cache is None:
                    cache = tracing_connector.get().contextmanager(cls_or_func, **outer_kwargs)
                with cache(*args, **kwargs) as val:
                    yield val

            return wrapper
        else:
            original_enter = cls_or_func.__enter__
            original_exit = cls_or_func.__exit__

            @wraps(original_enter)
            def one_shot_enter(*args, **kwargs):
                """Wrap after first call, tracing_connector was set up"""
                nonlocal cache
                if cache is None:
                    cls_or_func.__enter__ = original_enter
                    cls_or_func.__exit__ = original_exit
                    cache = tracing_connector.get().contextmanager(cls_or_func, **outer_kwargs)
                return cache.__enter__(*args, **kwargs)  # pylint: disable=unnecessary-dunder-call

            @wraps(original_exit)
            def one_shot_exit(*args, **kwargs):
                nonlocal cache
                return cache.__exit__(*args, **kwargs)

            setattr(cls_or_func, "__enter__", one_shot_enter)
            setattr(cls_or_func, "__exit__", one_shot_exit)

            return cls_or_func

    return decorator if arg is None else decorator(arg)


def class_methods(arg=None, /, **outer_kwargs):
    def decorator(cls):
        has_enter, has_exit = False, False

        for name, attr in inspect.getmembers(
            cls, lambda x: (inspect.isroutine(x) and not inspect.ismethoddescriptor(x) and not inspect.isbuiltin(x))
        ):
            if getattr(attr, "_disable_class_methods", False):
                continue
            if name == "__enter__":
                has_enter = True
            elif name == "__exit__":
                has_exit = True
            else:
                method = function(**outer_kwargs)(attr)
                if isinstance(inspect.getattr_static(cls, name), staticmethod):
                    method = staticmethod(method)
                setattr(cls, name, method)

        if all((has_enter, has_exit)):
            cls = contextmanager(**outer_kwargs)(cls)

        return cls

    return decorator if arg is None else decorator(arg)


def disable_class_methods(func):
    setattr(func, "_disable_class_methods", True)
    return func
