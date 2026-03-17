from os.path import abspath, join

from .conf import get_setting

import importlib.resources

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "project_dir",
    "PROJECT_DIR",
)


def project_dir(base: str) -> str:
    """Project dir."""
    return importlib.resources.files(__package__) / base


PROJECT_DIR = project_dir
