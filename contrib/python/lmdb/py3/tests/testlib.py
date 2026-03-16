#
# Copyright 2013 The py-lmdb authors, all rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted only as authorized by the OpenLDAP
# Public License.
#
# A copy of this license is available in the file LICENSE in the
# top-level directory of the distribution or, alternatively, at
# <http://www.OpenLDAP.org/license.html>.
#
# OpenLDAP is a registered trademark of the OpenLDAP Foundation.
#
# Individual files and/or contributed packages may be copyright by
# other parties and/or subject to additional restrictions.
#
# This work also contains materials derived from public sources.
#
# Additional information about OpenLDAP can be obtained at
# <http://www.openldap.org/>.
#

from __future__ import absolute_import
import atexit
import gc
import os
import shutil
import stat
import sys
import tempfile
import traceback
import unittest

try:
    import __builtin__
except ImportError:
    import builtins as __builtin__

import lmdb

_cleanups = []

def cleanup():
    while _cleanups:
        func = _cleanups.pop()
        try:
            func()
        except Exception:
            traceback.print_exc()

atexit.register(cleanup)

class LmdbTest(unittest.TestCase):
    def tearDown(self):
        cleanup()


def temp_dir(create=True):
    path = tempfile.mkdtemp(prefix='lmdb_test')
    assert path is not None, 'tempfile.mkdtemp failed'
    if not create:
        os.rmdir(path)
    _cleanups.append(lambda: shutil.rmtree(path, ignore_errors=True))
    if hasattr(path, 'decode'):
        path = path.decode(sys.getfilesystemencoding())
    return path


def temp_file(create=True):
    fd, path = tempfile.mkstemp(prefix='lmdb_test')
    assert path is not None, 'tempfile.mkstemp failed'
    os.close(fd)
    if not create:
        os.unlink(path)
    _cleanups.append(lambda: os.path.exists(path) and os.unlink(path))
    pathlock = path + '-lock'
    _cleanups.append(lambda: os.path.exists(pathlock) and os.unlink(pathlock))
    if hasattr(path, 'decode'):
        path = path.decode(sys.getfilesystemencoding())
    return path


def temp_env(path=None, max_dbs=10, **kwargs):
    if not path:
        path = temp_dir()
    env = lmdb.open(path, max_dbs=max_dbs, **kwargs)
    _cleanups.append(env.close)
    return path, env


def path_mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def debug_collect():
    if hasattr(gc, 'set_debug') and hasattr(gc, 'get_debug'):
        old = gc.get_debug()
        gc.set_debug(gc.DEBUG_LEAK)
        gc.collect()
        gc.set_debug(old)
    else:
        for x in range(10):
            # PyPy doesn't collect objects with __del__ on first attempt.
            gc.collect()

UnicodeType = getattr(__builtin__, 'unicode', str)
BytesType = getattr(__builtin__, 'bytes', str)

try:
    INT_TYPES = (int, long)
except NameError:
    INT_TYPES = (int,)

# B(ascii 'string') -> bytes
try:
    bytes('')     # Python>=2.6, alias for str().
    B = lambda s: s
except TypeError: # Python3.x, requires encoding parameter.
    B = lambda s: bytes(s, 'ascii')

# BL('s1', 's2') -> ['bytes1', 'bytes2']
BL = lambda *args: list(map(B, args))
# TS('s1', 's2') -> ('bytes1', 'bytes2')
BT = lambda *args: tuple(B(s) for s in args)
# O(int) -> length-1 bytes
O = lambda arg: B(chr(arg))
# OCT(s) -> parse string as octal
OCT = lambda s: int(s, 8)


KEYS = BL('a', 'b', 'baa', 'd')
ITEMS = [(k, B('')) for k in KEYS]
REV_ITEMS = ITEMS[::-1]
VALUES = [B('') for k in KEYS]

KEYS2 = BL('a', 'b', 'baa', 'd', 'e', 'f', 'g', 'h')
ITEMS2 = [(k, B('')) for k in KEYS2]
REV_ITEMS2 = ITEMS2[::-1]
VALUES2 = [B('') for k in KEYS2]

KEYSFIXED = BL('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h')
VALUES_MULTI = [(B('r'), B('s')) for k in KEYSFIXED]
ITEMS_MULTI_FIXEDKEY = [
    (kv[0], v) for kv in list(zip(KEYSFIXED, VALUES_MULTI)) for v in kv[1]
    ]

def _put_items(items, t, db=None):
    for k, v in items:
        if db:
            t.put(k, v, db=db)
        else:
            t.put(k, v)


def putData(t, db=None):
    _put_items(ITEMS, t, db=db)

def putBigData(t, db=None):
    _put_items(ITEMS2, t, db=db)

def putBigDataMultiFixed(t, db=None):
    _put_items(ITEMS_MULTI_FIXEDKEY, t, db=db)