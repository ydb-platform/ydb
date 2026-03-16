"""EditorConfig exception classes

Licensed under Simplified BSD License (see LICENSE.BSD file).

"""


class EditorConfigError(Exception):
    """Parent class of all exceptions raised by EditorConfig"""


from configparser import ParsingError as _ParsingError


class ParsingError(_ParsingError, EditorConfigError):
    """Error raised if an EditorConfig file could not be parsed"""


class PathError(ValueError, EditorConfigError):
    """Error raised if invalid filepath is specified"""


class VersionError(ValueError, EditorConfigError):
    """Error raised if invalid version number is specified"""
