from unittest import TestCase

from webauthn.helpers import parse_backup_flags
from webauthn.helpers.structs import AuthenticatorDataFlags
from webauthn.helpers.exceptions import InvalidBackupFlags


class TestParseBackupFlags(TestCase):
    flags: AuthenticatorDataFlags

    def setUp(self) -> None:
        self.flags = AuthenticatorDataFlags(
            up=True,
            uv=False,
            be=False,
            bs=False,
            at=False,
            ed=False,
        )

    def test_returns_single_device_not_backed_up(self) -> None:
        self.flags.be = False
        self.flags.bs = False

        parsed = parse_backup_flags(self.flags)

        self.assertEqual(parsed.credential_device_type, "single_device")
        self.assertEqual(parsed.credential_backed_up, False)

    def test_returns_multi_device_not_backed_up(self) -> None:
        self.flags.be = True
        self.flags.bs = False

        parsed = parse_backup_flags(self.flags)

        self.assertEqual(parsed.credential_device_type, "multi_device")
        self.assertEqual(parsed.credential_backed_up, False)

    def test_returns_multi_device_backed_up(self) -> None:
        self.flags.be = True
        self.flags.bs = True

        parsed = parse_backup_flags(self.flags)

        self.assertEqual(parsed.credential_device_type, "multi_device")
        self.assertEqual(parsed.credential_backed_up, True)

    def test_raises_on_invalid_backup_state_flags(self) -> None:
        self.flags.be = False
        self.flags.bs = True

        with self.assertRaisesRegex(
            InvalidBackupFlags,
            "impossible",
        ):
            parse_backup_flags(self.flags)
