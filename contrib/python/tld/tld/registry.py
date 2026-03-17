import warnings

from .base import Registry  # noqa

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = ("Registry",)

warnings.warn(
    "The `Registry` class is moved from `tld.registry` to `tld.base`.",
    DeprecationWarning,
)
