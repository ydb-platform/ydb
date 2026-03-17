# -*- coding: utf-8 -*-
"""Contains the main `APISpec` class.
"""
from .core import APISpec, Path
from .plugin import BasePlugin

__version__ = '0.39.0'
__author__ = 'Steven Loria, Jérôme Lafréchoux, and contributors'
__license__ = 'MIT'


__all__ = [
    'APISpec',
    'Path',
    'BasePlugin',
]
