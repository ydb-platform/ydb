#!/usr/bin/env python

"""Unit tests for M2Crypto.SSL.timeout.
"""

import sys
from M2Crypto.SSL import timeout, struct_to_timeout, struct_size
from tests import unittest

# Max value for sec argument on Windows:
# - needs to fit DWORD (signed 32-bit) when converted to millisec
MAX_SEC_WIN32 = int((2**31 - 1) / 1000)

# Max value for sec argument on other platforms:
# Note: It may actually be 64-bit but we are happy with 32-bit.
# We use the signed maximum, because the packing uses lower case "l".
MAX_SEC_OTHER = 2**31 - 1

# Enable this to test the Windows logic on a non-Windows platform:
# sys.platform = 'win32'


class TimeoutTestCase(unittest.TestCase):

    def timeout_test(self, sec, microsec, exp_sec=None, exp_microsec=None):
        """
        Test that the timeout values (sec, microsec) are the same after
        round tripping through a pack / unpack cycle.
        """
        if exp_sec is None:
            exp_sec = sec
        if exp_microsec is None:
            exp_microsec = microsec

        to = timeout(sec, microsec)

        binstr = to.pack()

        act_to = struct_to_timeout(binstr)

        self.assertEqual(
            (act_to.sec, act_to.microsec), (exp_sec, exp_microsec),
            "Unexpected timeout(sec,microsec) after pack + unpack: "
            "Got (%r,%r), expected (%r,%r), input was (%r,%r)" %
            (act_to.sec, act_to.microsec, exp_sec, exp_microsec,
            sec, microsec))

    def test_timeout_0_0(self):
        self.timeout_test(0, 0)

    def test_timeout_123_0(self):
        self.timeout_test(123, 0)

    def test_timeout_max_0(self):
        if sys.platform == 'win32':
            self.timeout_test(MAX_SEC_WIN32, 0)
        else:
            self.timeout_test(MAX_SEC_OTHER, 0)

    def test_timeout_0_456000(self):
        self.timeout_test(0, 456000)

    def test_timeout_123_456000(self):
        self.timeout_test(123, 456000)

    def test_timeout_2_3000000(self):
        if sys.platform == 'win32':
            self.timeout_test(2, 3000000, 5, 0)
        else:
            self.timeout_test(2, 3000000)

    def test_timeout_2_2499000(self):
        if sys.platform == 'win32':
            self.timeout_test(2, 2499000, 4, 499000)
        else:
            self.timeout_test(2, 2499000)

    def test_timeout_2_2999000(self):
        if sys.platform == 'win32':
            self.timeout_test(2, 2999000, 4, 999000)
        else:
            self.timeout_test(2, 2999000)

    def test_timeout_max_456000(self):
        if sys.platform == 'win32':
            self.timeout_test(MAX_SEC_WIN32, 456000)
        else:
            self.timeout_test(MAX_SEC_OTHER, 456000)

    def test_timeout_0_456(self):
        if sys.platform == 'win32':
            self.timeout_test(0, 456, None, 0)
        else:
            self.timeout_test(0, 456)

    def test_timeout_123_456(self):
        if sys.platform == 'win32':
            self.timeout_test(123, 456, None, 0)
        else:
            self.timeout_test(123, 456)

    def test_timeout_max_456(self):
        if sys.platform == 'win32':
            self.timeout_test(MAX_SEC_WIN32, 456, None, 0)
        else:
            self.timeout_test(MAX_SEC_OTHER, 456)

    def test_timeout_1_499(self):
        if sys.platform == 'win32':
            self.timeout_test(123, 499, None, 0)  # 499 us rounds down to 0
        else:
            self.timeout_test(123, 499)

    def test_timeout_1_501(self):
        # We use 501 for this test and not 500 because 0.5 is not exactly
        # represented in binary floating point numbers, and because 0.5
        # rounds differently between py2 and py3. See Python round() docs.
        if sys.platform == 'win32':
            self.timeout_test(123, 501, None, 1000)  # 501 us rounds up to 1000
        else:
            self.timeout_test(123, 501)

    def test_timeout_size(self):
        exp_size = len(timeout(0, 0).pack())
        self.assertEqual(struct_size(), exp_size)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TimeoutTestCase))
    return suite


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
