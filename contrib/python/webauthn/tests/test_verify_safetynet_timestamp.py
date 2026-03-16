from unittest import TestCase
from unittest.mock import MagicMock, patch

from webauthn.helpers import verify_safetynet_timestamp


class TestVerifySafetyNetTimestamp(TestCase):
    mock_time: MagicMock
    # time.time() returns time in microseconds
    mock_now = 1636589648

    def setUp(self) -> None:
        super().setUp()
        time_patch = patch("time.time")
        self.mock_time = time_patch.start()
        self.mock_time.return_value = self.mock_now

    def tearDown(self) -> None:
        super().tearDown()
        patch.stopall()

    def test_does_not_raise_on_timestamp_slightly_in_future(self):
        # Put timestamp just a bit in the future
        timestamp_ms = (self.mock_now * 1000) + 600
        verify_safetynet_timestamp(timestamp_ms)

        assert True

    def test_does_not_raise_on_timestamp_slightly_in_past(self):
        # Put timestamp just a bit in the past
        timestamp_ms = (self.mock_now * 1000) - 600
        verify_safetynet_timestamp(timestamp_ms)

        assert True

    def test_raises_on_timestamp_too_far_in_future(self):
        # Put timestamp 20 seconds in the future
        timestamp_ms = (self.mock_now * 1000) + 20000
        self.assertRaisesRegex(
            ValueError,
            "was later than",
            lambda: verify_safetynet_timestamp(timestamp_ms),
        )

    def test_raises_on_timestamp_too_far_in_past(self):
        # Put timestamp 20 seconds in the past
        timestamp_ms = (self.mock_now * 1000) - 20000
        self.assertRaisesRegex(
            ValueError,
            "expired",
            lambda: verify_safetynet_timestamp(timestamp_ms),
        )

    def test_does_not_raise_on_last_possible_millisecond(self):
        # Timestamp is verified at the exact last millisecond
        timestamp_ms = (self.mock_now * 1000) + 10000
        verify_safetynet_timestamp(timestamp_ms)

        assert True
