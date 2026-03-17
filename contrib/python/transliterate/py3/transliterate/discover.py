import os

from six import print_

try:
    from importlib import import_module
except ImportError:
    import_module = __import__

from .conf import get_setting
from .helpers import PROJECT_DIR

__title__ = 'transliterate.discover'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('autodiscover',)


def autodiscover():
    """Auto-discover the language packs in contrib/apps."""

    import sys

    for module_name in sys.extra_modules:
        if module_name.startswith('transliterate.contrib.languages') and module_name.endswith('translit_language_pack'):
            import_module(module_name)
