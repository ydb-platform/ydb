#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
# Suppress: import positions after some globals are defined
# pylint: disable=wrong-import-position
"""A module to aggregate config parser (loader/dumper) backends.
"""
from __future__ import absolute_import

import logging

import anyconfig.compat
import anyconfig.ioinfo
import anyconfig.processors
import anyconfig.singleton
import anyconfig.utils

import anyconfig.backend.base
import anyconfig.backend.ini
import anyconfig.backend.json
import anyconfig.backend.pickle
import anyconfig.backend.properties
import anyconfig.backend.shellvars
import anyconfig.backend.yaml
import anyconfig.backend.xml


LOGGER = logging.getLogger(__name__)
PARSERS = [anyconfig.backend.ini.Parser,
           anyconfig.backend.pickle.Parser,
           anyconfig.backend.properties.Parser,
           anyconfig.backend.shellvars.Parser, anyconfig.backend.xml.Parser]

PARSERS.extend(anyconfig.backend.json.PARSERS)

_NA_MSG = "%s is not available. Disabled %s support."

if anyconfig.backend.yaml.PARSERS:
    PARSERS.extend(anyconfig.backend.yaml.PARSERS)
else:
    LOGGER.info(_NA_MSG, "yaml module", "YAML")

try:
    import anyconfig.backend.toml
    PARSERS.append(anyconfig.backend.toml.Parser)
except ImportError:
    LOGGER.info(_NA_MSG, "toml module", "TOML")


class Parsers(anyconfig.processors.Processors,
              anyconfig.singleton.Singleton):
    """
    Manager class for parsers.
    """
    _pgroup = "anyconfig_backends"

    def __init__(self, processors=None):
        """Initialize with PARSERS.
        """
        if processors is None:
            processors = PARSERS

        super(Parsers, self).__init__(processors)

# vim:sw=4:ts=4:et:
