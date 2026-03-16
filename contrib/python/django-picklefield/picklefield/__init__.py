import django.utils.version

from .constants import DEFAULT_PROTOCOL
from .fields import PickledObjectField

__all__ = 'VERSION', '__version__', 'DEFAULT_PROTOCOL', 'PickledObjectField'

VERSION = (3, 4, 0, 'final', 0)

__version__ = django.utils.version.get_version(VERSION)
