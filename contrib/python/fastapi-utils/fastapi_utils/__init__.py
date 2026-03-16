import importlib.metadata
import warnings

from .cbv_base import Api, Resource, set_responses, take_init_parameters

try:
    __version__ = "0.8.0"
except importlib.metadata.PackageNotFoundError as e:
    warnings.warn(f"Could not determine version of {__name__}", stacklevel=1)
    warnings.warn(str(e), stacklevel=1)
    __version__ = "0.8.0"


__all__ = [
    "Api",
    "Resource",
    "set_responses",
    "take_init_parameters",
]
