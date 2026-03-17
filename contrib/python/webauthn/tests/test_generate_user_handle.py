from unittest import TestCase

from webauthn.helpers import generate_user_handle


class TestWebAuthnGenerateUserHandle(TestCase):
    def test_generates_byte_sequence(self) -> None:
        output = generate_user_handle()

        assert type(output) == bytes
        assert len(output) == 64

    def test_generates_unique_value_each_time(self) -> None:
        output1 = generate_user_handle()
        output2 = generate_user_handle()

        assert output1 != output2
