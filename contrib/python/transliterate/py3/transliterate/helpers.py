import os
import sys

__title__ = 'transliterate.helpers'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = (
    'PROJECT_DIR',
    'project_dir',
    'PY32',
)

try:
    PY32 = (sys.version_info[0] == 3 and sys.version_info[1] == 2)
except Exception as err:
    PY32 = False


def project_dir(base):
    """Project dir."""
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            (os.path.join(*base) if isinstance(base, (list, tuple)) else base)
        ).replace('\\', '/')
    )


PROJECT_DIR = project_dir
