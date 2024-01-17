#
# This file is part of pyasn1-modules software.
#
import sys
import unittest

# modules without tests
from pyasn1_modules import (
    rfc1155, rfc1157, rfc1901, rfc3412, rfc3414
)

suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    import sys

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
