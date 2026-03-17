from __future__ import annotations

import typing as t

from .extension import SQLAlchemy

__version__ = "3.0.5"

__all__ = [
    "SQLAlchemy",
]

_deprecated_map = {
    "Model": ".model.Model",
    "DefaultMeta": ".model.DefaultMeta",
    "Pagination": ".pagination.Pagination",
    "BaseQuery": ".query.Query",
    "get_debug_queries": ".record_queries.get_recorded_queries",
    "SignallingSession": ".session.Session",
    "before_models_committed": ".track_modifications.before_models_committed",
    "models_committed": ".track_modifications.models_committed",
}


def __getattr__(name: str) -> t.Any:
    import importlib
    import warnings

    if name in _deprecated_map:
        path = _deprecated_map[name]
        import_path, _, new_name = path.rpartition(".")
        action = "moved and renamed"

        if new_name == name:
            action = "moved"

        warnings.warn(
            f"'{name}' has been {action} to '{path[1:]}'. The top-level import is"
            " deprecated and will be removed in Flask-SQLAlchemy 3.1.",
            DeprecationWarning,
            stacklevel=2,
        )
        mod = importlib.import_module(import_path, __name__)
        return getattr(mod, new_name)

    raise AttributeError(name)
