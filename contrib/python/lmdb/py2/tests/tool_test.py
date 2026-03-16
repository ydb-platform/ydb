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

import sys
import shlex
import unittest

import lmdb
import lmdb.tool
import testlib

def call_tool(cmdline):
    if sys.platform == 'win32':
        args = cmdline.split()
    else:
        args = shlex.split(cmdline)
    return lmdb.tool.main(args)

class ToolTest(testlib.LmdbTest):
    def test_cmd_get(self):
        frompath, env = testlib.temp_env()
        db = env.open_db(b'subdb')
        with env.begin(write=True, db=db) as txn:
            txn.put(b'foo', b'bar', db=db)
        env.close()
        call_tool('-d subdb get --env %s' % (frompath,))

    def test_cmd_rewrite(self):
        frompath, env = testlib.temp_env()
        env.open_db(b'subdb')
        env.close()
        topath = testlib.temp_dir()
        call_tool('rewrite -e %s -E %s subdb' % (frompath, topath))

if __name__ == '__main__':
    unittest.main()
