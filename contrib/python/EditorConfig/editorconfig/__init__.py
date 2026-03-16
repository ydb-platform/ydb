"""EditorConfig Python Core"""

from collections import OrderedDict

from editorconfig.versiontools import join_version
from editorconfig.version import VERSION

__all__ = ['get_properties', 'EditorConfigError', 'exceptions']

__version__ = join_version(VERSION)


def get_properties(filename: str) -> OrderedDict[str, str]:
    """Locate and parse EditorConfig files for the given filename"""
    handler = EditorConfigHandler(filename)
    return handler.get_configurations()


from editorconfig.handler import EditorConfigHandler
from editorconfig.exceptions import *
