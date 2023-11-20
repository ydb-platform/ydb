# Copyright (c) 2017-present, Gregory Szorc
# All rights reserved.
#
# This software may be modified and distributed under the terms
# of the BSD license. See the LICENSE file for details.

"""Python interface to the Zstandard (zstd) compression library."""

from __future__ import absolute_import, unicode_literals

# This module serves 2 roles:
#
# 1) Export the C or CFFI "backend" through a central module.
# 2) Implement additional functionality built on top of C or CFFI backend.

import os
import platform

# Some Python implementations don't support C extensions. That's why we have
# a CFFI implementation in the first place. The code here import one of our
# "backends" then re-exports the symbols from this module. For convenience,
# we support falling back to the CFFI backend if the C extension can't be
# imported. But for performance reasons, we only do this on unknown Python
# implementation. Notably, for CPython we require the C extension by default.
# Because someone will inevitably want special behavior, the behavior is
# configurable via an environment variable. A potentially better way to handle
# this is to import a special ``__importpolicy__`` module or something
# defining a variable and `setup.py` could write the file with whatever
# policy was specified at build time. Until someone needs it, we go with
# the hacky but simple environment variable approach.
_module_policy = os.environ.get("PYTHON_ZSTANDARD_IMPORT_POLICY", "default")

if _module_policy == "default":
    if platform.python_implementation() in ("CPython",):
        from zstd import *

        backend = "cext"
    elif platform.python_implementation() in ("PyPy",):
        from .cffi import *

        backend = "cffi"
    else:
        try:
            from zstd import *

            backend = "cext"
        except ImportError:
            from .cffi import *

            backend = "cffi"
elif _module_policy == "cffi_fallback":
    try:
        from zstd import *

        backend = "cext"
    except ImportError:
        from .cffi import *

        backend = "cffi"
elif _module_policy == "cext":
    from zstd import *

    backend = "cext"
elif _module_policy == "cffi":
    from .cffi import *

    backend = "cffi"
else:
    raise ImportError(
        "unknown module import policy: %s; use default, cffi_fallback, "
        "cext, or cffi" % _module_policy
    )

# Keep this in sync with python-zstandard.h.
__version__ = "0.14.1"
