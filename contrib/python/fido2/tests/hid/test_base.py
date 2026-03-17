import unittest
from fido2.hid.base import parse_report_descriptor


class TestBase(unittest.TestCase):
    def test_parse_report_descriptor_1(self):
        max_in_size, max_out_size = parse_report_descriptor(
            bytes.fromhex(
                "06d0f10901a1010920150026ff007508954081020921150026ff00750895409102c0"
            )
        )

        self.assertEqual(max_in_size, 64)
        self.assertEqual(max_out_size, 64)

    def test_parse_report_descriptor_2(self):
        with self.assertRaises(ValueError):
            parse_report_descriptor(
                bytes.fromhex(
                    "05010902a1010901a10005091901290515002501950575018102950175038101"
                    "05010930093109381581257f750895038106c0c0"
                )
            )
