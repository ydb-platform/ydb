####################################################################
#                                                                  #
# THIS FILE IS PART OF THE pycollada LIBRARY SOURCE CODE.          #
# USE, DISTRIBUTION AND REPRODUCTION OF THIS LIBRARY SOURCE IS     #
# GOVERNED BY A BSD-STYLE SOURCE LICENSE INCLUDED WITH THIS SOURCE #
# IN 'COPYING'. PLEASE READ THESE TERMS BEFORE DISTRIBUTING.       #
#                                                                  #
# THE pycollada SOURCE CODE IS (C) COPYRIGHT 2011                  #
# by Jeff Terrace and contributors                                 #
#                                                                  #
####################################################################

import os
import sys
import unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


if __name__ == '__main__':
    suite = unittest.TestLoader().discover("collada.tests")
    ret = unittest.TextTestRunner(verbosity=2).run(suite)
    if ret.wasSuccessful():
        sys.exit(0)
    sys.exit(1)
