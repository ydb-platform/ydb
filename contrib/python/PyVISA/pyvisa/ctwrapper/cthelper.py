# -*- coding: utf-8 -*-
"""Cross platform helper of ctypes.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import ctypes
import os
import sys

if sys.platform == "win32":
    FUNCTYPE, Library = ctypes.WINFUNCTYPE, ctypes.WinDLL
else:
    FUNCTYPE, Library = ctypes.CFUNCTYPE, ctypes.CDLL

# On Linux, find Library returns the name not the path.
# This excerpt provides a modified find_library.
if os.name == "posix" and sys.platform.startswith("linux"):
    # Andreas Degert's find functions, using gcc, /sbin/ldconfig, objdump
    def define_find_libary():
        import errno
        import re
        import tempfile

        def _findlib_gcc(name):
            expr = r"[^\(\)\s]*lib%s\.[^\(\)\s]*" % re.escape(name)
            fdout, ccout = tempfile.mkstemp()
            os.close(fdout)
            cmd = (
                "if type gcc >/dev/null 2>&1; then CC=gcc; else CC=cc; fi;"
                "$CC -Wl,-t -o " + ccout + " 2>&1 -l" + name
            )
            trace = ""
            try:
                f = os.popen(cmd)
                trace = f.read()
                f.close()
            finally:
                try:
                    os.unlink(ccout)
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise
            res = re.search(expr, trace)
            if not res:
                return None
            return res.group(0)

        def _findlib_ldconfig(name):
            # NOTE: assuming GLIBC's ldconfig (with option -p)
            expr = r"/[^\(\)\s]*lib%s\.[^\(\)\s]*" % re.escape(name)
            with os.popen("/sbin/ldconfig -p 2>/dev/null") as pipe:
                res = re.search(expr, pipe.read())
            if not res:
                # Hm, this works only for libs needed by the python executable.
                cmd = "ldd %s 2>/dev/null" % sys.executable
                with os.popen(cmd) as pipe:
                    res = re.search(expr, pipe.read())
                if not res:
                    return None
            return res.group(0)

        def _find_library(name):
            path = _findlib_ldconfig(name) or _findlib_gcc(name)
            if path:
                return os.path.realpath(path)
            return path

        return _find_library

    find_library = define_find_libary()
else:
    from ctypes.util import find_library

__all__ = ["FUNCTYPE", "Library", "find_library"]
