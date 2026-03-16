from os.path import dirname

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "DEBUG",
    "NAMES_LOCAL_PATH_PARENT",
)

# Absolute base path that is prepended to NAMES_LOCAL_PATH
NAMES_LOCAL_PATH_PARENT = dirname(__file__)

DEBUG = False
