from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponseBase

    # A generic Django view function
    _VIEW_T = Callable[[HttpRequest], HttpResponseBase]
    _VIEW_DECORATOR_T = Callable[[_VIEW_T], _VIEW_T]


def csp_exempt(REPORT_ONLY: bool | None = None) -> _VIEW_DECORATOR_T:
    if callable(REPORT_ONLY):
        raise RuntimeError(
            "Incompatible `csp_exempt` decorator usage. This decorator now requires arguments, "
            "even if none are passed. Change bare decorator usage (@csp_exempt) to parameterized "
            "decorator usage (@csp_exempt()). See the django-csp 4.0 migration guide for more "
            "information."
        )

    def decorator(f: _VIEW_T) -> _VIEW_T:
        @wraps(f)
        def _wrapped(*a: Any, **kw: Any) -> HttpResponseBase:
            resp = f(*a, **kw)
            if REPORT_ONLY:
                setattr(resp, "_csp_exempt_ro", True)
            else:
                setattr(resp, "_csp_exempt", True)
            return resp

        return _wrapped

    return decorator


# Error message for deprecated decorator arguments.
DECORATOR_DEPRECATION_ERROR = (
    "Incompatible `{fname}` decorator arguments. This decorator now takes a single dict argument. "
    "See the django-csp 4.0 migration guide for more information."
)


def csp_update(config: dict[str, Any] | None = None, REPORT_ONLY: bool = False, **kwargs: Any) -> _VIEW_DECORATOR_T:
    if config is None and kwargs:
        raise RuntimeError(DECORATOR_DEPRECATION_ERROR.format(fname="csp_update"))

    def decorator(f: _VIEW_T) -> _VIEW_T:
        @wraps(f)
        def _wrapped(*a: Any, **kw: Any) -> HttpResponseBase:
            resp = f(*a, **kw)
            if REPORT_ONLY:
                setattr(resp, "_csp_update_ro", config)
            else:
                setattr(resp, "_csp_update", config)
            return resp

        return _wrapped

    return decorator


def csp_replace(config: dict[str, Any] | None = None, REPORT_ONLY: bool = False, **kwargs: Any) -> _VIEW_DECORATOR_T:
    if config is None and kwargs:
        raise RuntimeError(DECORATOR_DEPRECATION_ERROR.format(fname="csp_replace"))

    def decorator(f: _VIEW_T) -> _VIEW_T:
        @wraps(f)
        def _wrapped(*a: Any, **kw: Any) -> HttpResponseBase:
            resp = f(*a, **kw)
            if REPORT_ONLY:
                setattr(resp, "_csp_replace_ro", config)
            else:
                setattr(resp, "_csp_replace", config)
            return resp

        return _wrapped

    return decorator


def csp(config: dict[str, Any] | None = None, REPORT_ONLY: bool = False, **kwargs: Any) -> _VIEW_DECORATOR_T:
    if config is None and kwargs:
        raise RuntimeError(DECORATOR_DEPRECATION_ERROR.format(fname="csp"))

    if config is None:
        processed_config: dict[str, list[Any]] = {}
    else:
        processed_config = {k: [v] if isinstance(v, str) else v for k, v in config.items()}

    def decorator(f: _VIEW_T) -> _VIEW_T:
        @wraps(f)
        def _wrapped(*a: Any, **kw: Any) -> HttpResponseBase:
            resp = f(*a, **kw)
            if REPORT_ONLY:
                setattr(resp, "_csp_config_ro", processed_config)
            else:
                setattr(resp, "_csp_config", processed_config)
            return resp

        return _wrapped

    return decorator
