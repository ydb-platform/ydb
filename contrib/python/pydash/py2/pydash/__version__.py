# -*- coding: utf-8 -*-
"""Project version information."""

from pkg_resources import DistributionNotFound, get_distribution


try:
    __version__ = get_distribution(__name__.split(".")[0]).version
except DistributionNotFound:  # pragma: no cover
    # Package is not installed.
    __version__ = None
