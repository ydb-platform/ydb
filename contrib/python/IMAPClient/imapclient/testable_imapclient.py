# Copyright (c) 2014, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

from typing import Any, Dict, List
from unittest.mock import Mock

from .imapclient import IMAPClient


class TestableIMAPClient(IMAPClient):
    """Wrapper of :py:class:`imapclient.IMAPClient` that mocks all
    interaction with real IMAP server.

    This class should only be used in tests, where you can safely
    interact with imapclient without running commands on a real
    IMAP account.
    """

    def __init__(self) -> None:
        super().__init__("somehost")

    def _create_IMAP4(self) -> "MockIMAP4":
        return MockIMAP4()


class MockIMAP4(Mock):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.use_uid = True
        self.sent = b""  # Accumulates what was given to send()
        self.tagged_commands: Dict[Any, Any] = {}
        self.untagged_responses: Dict[Any, Any] = {}
        self.capabilities: List[str] = []
        self._starttls_done = False

    def send(self, data: bytes) -> None:
        self.sent += data

    def _new_tag(self) -> str:
        return "tag"
