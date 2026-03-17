import warnings

from .loaders import *  # noqa

warnings.warn(
    "The 'webpack_loader.loader' module has been renamed to 'webpack_loader.loaders'. "
    "Please update your imports and config to use the new module.",
    DeprecationWarning,
)
