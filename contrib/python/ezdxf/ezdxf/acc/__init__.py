#  Copyright (c) 2020-2024, Manfred Moitzi
#  License: MIT License
import sys
from ezdxf._options import options

# Set environment variable EZDXF_DISABLE_C_EXT to '1' or 'True' to disable
# the usage of C extensions implemented by Cython.
#
# Important: If you change the EZDXF_DISABLE_C_EXT state, you have to restart
# the Python interpreter, because C extension integration is done at the
# ezdxf import!
#
# Config files:
# Section: core
# Key: disable_c_ext = 1
#
# Direct imports from the C extension modules can not be disabled,
# just the usage by the ezdxf core package.
# For an example see ezdxf.math.__init__, if you import Vec3 from ezdxf.math
# the implementation depends on DISABLE_C_EXT and the existence of the C
# extension, but if you import Vec3 from ezdxf.math.vectors, you always get
# the Python implementation.

USE_C_EXT = not options.disable_c_ext

# C-extensions are always disabled for pypy because JIT compiled Python code is
# much faster!
PYPY = hasattr(sys, 'pypy_version_info')
if PYPY:
    USE_C_EXT = False

if USE_C_EXT:
    try:
        from ezdxf.acc import vector
    except ImportError:
        USE_C_EXT = False

# set actual state of C-extension usage
options._use_c_ext = USE_C_EXT
