import sys

# Python version flags for Python 3 support
python_major_version = 0
if hasattr(sys.version_info, 'major'):
  python_major_version = sys.version_info.major
else:
  python_major_version = sys.version_info[0]

PY2 = ( python_major_version == 2 )
PY3 = ( python_major_version == 3 )  

from .util import *
