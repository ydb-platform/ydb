import warnings

from bottle import Bottle
from optuna.storages import BaseStorage

from ._app import create_app as _create_app


def create_app(storage: BaseStorage) -> Bottle:
    warnings.warn(
        "This function will be removed in the future."
        " Please use optuna_dashboard.run_server() instead.",
        DeprecationWarning,
    )
    return _create_app(storage)
