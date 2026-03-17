#
# Copyright (C) 2013 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh @ gmail.com>
# License: MIT
#
# pylint: disable=invalid-name
"""anyconfig globals.
"""
import collections
import anyconfig.init


PACKAGE = "anyconfig"
AUTHOR = "Satoru SATOH <satoru.satoh@gmail.com>"
VERSION = "0.9.11"

LOGGER = anyconfig.init.getLogger(PACKAGE)

IOI_KEYS = "src type path opener extension".split()
IOInfo = collections.namedtuple("IOInfo", IOI_KEYS)

IOI_TYPES = (IOI_NONE, IOI_PATH_STR, IOI_PATH_OBJ, IOI_STREAM) = \
            (None, "path", "pathlib.Path", "stream")


class UnknownParserTypeError(RuntimeError):
    """Raise if no parsers were found for given type."""
    def __init__(self, forced_type):
        msg = "No parser found for type '%s'" % forced_type
        super(UnknownParserTypeError, self).__init__(msg)


class UnknownProcessorTypeError(RuntimeError):
    """Raise if no processors were found for given type."""
    def __init__(self, forced_type):
        msg = "No parser found for type '%s'" % forced_type
        super(UnknownProcessorTypeError, self).__init__(msg)


class UnknownFileTypeError(RuntimeError):
    """Raise if not parsers were found for given file path."""
    def __init__(self, path):
        msg = "No parser found for file '%s'" % path
        super(UnknownFileTypeError, self).__init__(msg)

# vim:sw=4:ts=4:et:
