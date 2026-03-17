from unittest import TestCase

from webauthn.helpers.tpm import map_tpm_manufacturer_id


class TestWebAuthnGenerateUserHandle(TestCase):
    def test_handles_recognized_id(self) -> None:
        info = map_tpm_manufacturer_id("id:4353434F")

        self.assertEqual(info.name, "Cisco")
        self.assertEqual(info.id, "CSCO")

    def test_raises_on_unrecognized_id(self) -> None:
        with self.assertRaises(KeyError):
            map_tpm_manufacturer_id("id:FFFFFFFF")
