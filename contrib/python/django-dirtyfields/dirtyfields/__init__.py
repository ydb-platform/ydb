"""
django-dirtyfields library for tracking dirty fields on a Model instance.

Adapted from https://stackoverflow.com/questions/110803/dirty-fields-in-django
"""

__all__ = ['DirtyFieldsMixin']
__version__ = "1.9.9"
from dirtyfields.dirtyfields import DirtyFieldsMixin

VERSION = tuple(map(int, __version__.split(".")[0:3]))
