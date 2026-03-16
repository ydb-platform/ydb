# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Maintains a registry of transport mechanisms.

The main entrypoint is :func:`get_transport`.  See also :file:`extending.md`.

"""
import importlib
import logging

import smart_open.local_file

logger = logging.getLogger(__name__)

NO_SCHEME = ''

_REGISTRY = {NO_SCHEME: smart_open.local_file}
_ERRORS = {}
_MISSING_DEPS_ERROR = """You are trying to use the %(module)s functionality of smart_open
but you do not have the correct %(module)s dependencies installed. Try:

    pip install smart_open[%(module)s]

"""


def register_transport(submodule):
    """Register a submodule as a transport mechanism for ``smart_open``.

    This module **must** have:

        - `SCHEME` attribute (or `SCHEMES`, if the submodule supports multiple schemes)
        - `open` function
        - `open_uri` function
        - `parse_uri' function

    Once registered, you can get the submodule by calling :func:`get_transport`.

    """
    module_name = submodule
    if isinstance(submodule, str):
        try:
            submodule = importlib.import_module(submodule)
        except ImportError:
            return
    else:
        module_name = submodule.__name__
    # Save only the last module name piece
    module_name = module_name.rsplit(".")[-1]

    if hasattr(submodule, "SCHEME"):
        schemes = [submodule.SCHEME]
    elif hasattr(submodule, "SCHEMES"):
        schemes = submodule.SCHEMES
    else:
        raise ValueError("%r does not have a .SCHEME or .SCHEMES attribute" % submodule)

    for f in ("open", "open_uri", "parse_uri"):
        assert hasattr(submodule, f), "%r is missing %r" % (submodule, f)

    for scheme in schemes:
        assert scheme not in _REGISTRY
        if getattr(submodule, "MISSING_DEPS", False):
            _ERRORS[scheme] = module_name
        else:
            _REGISTRY[scheme] = submodule


def get_transport(scheme):
    """Get the submodule that handles transport for the specified scheme.

    This submodule must have been previously registered via :func:`register_transport`.

    """
    expected = SUPPORTED_SCHEMES
    readme_url = (
        "https://github.com/piskvorky/smart_open/blob/master/README.rst"
    )
    message = (
        "Unable to handle scheme %(scheme)r, expected one of %(expected)r. "
        "Extra dependencies required by %(scheme)r may be missing. "
        "See <%(readme_url)s> for details." % locals()
    )
    if scheme in _ERRORS:
        raise ImportError(_MISSING_DEPS_ERROR % dict(module=_ERRORS[scheme]))
    if scheme in _REGISTRY:
        return _REGISTRY[scheme]
    raise NotImplementedError(message)


register_transport(smart_open.local_file)
register_transport("smart_open.azure")
register_transport("smart_open.ftp")
register_transport("smart_open.gcs")
register_transport("smart_open.hdfs")
register_transport("smart_open.http")
register_transport("smart_open.s3")
register_transport("smart_open.ssh")
register_transport("smart_open.webhdfs")

SUPPORTED_SCHEMES = tuple(sorted(_REGISTRY.keys()))
"""The transport schemes that the local installation of ``smart_open`` supports."""
