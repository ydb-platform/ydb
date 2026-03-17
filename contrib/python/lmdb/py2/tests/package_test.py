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
import unittest

import lmdb


class PackageExportsTest(unittest.TestCase):
    """
    Ensure the list of exported names matches a predefined list. Designed to
    ensure future interface changes to cffi.py and cpython.c don't break
    consistency of "from lmdb import *".
    """
    def test_exports(self):
        assert sorted(lmdb.__all__) == [
            'BadDbiError',
            'BadRslotError',
            'BadTxnError',
            'BadValsizeError',
            'CorruptedError',
            'Cursor',
            'CursorFullError',
            'DbsFullError',
            'DiskError',
            'Environment',
            'Error',
            'IncompatibleError',
            'InvalidError',
            'InvalidParameterError',
            'KeyExistsError',
            'LockError',
            'MapFullError',
            'MapResizedError',
            'MemoryError',
            'NotFoundError',
            'PageFullError',
            'PageNotFoundError',
            'PanicError',
            'ReadersFullError',
            'ReadonlyError',
            'TlsFullError',
            'Transaction',
            'TxnFullError',
            'VersionMismatchError',
            '_Database',
            'enable_drop_gil',
            'version',
        ]
