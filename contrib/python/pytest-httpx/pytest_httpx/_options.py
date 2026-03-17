from typing import Callable

import httpx


class _HTTPXMockOptions:
    def __init__(
        self,
        *,
        assert_all_responses_were_requested: bool = True,
        assert_all_requests_were_expected: bool = True,
        can_send_already_matched_responses: bool = False,
        should_mock: Callable[[httpx.Request], bool] = lambda request: True,
    ) -> None:
        self.assert_all_responses_were_requested = assert_all_responses_were_requested
        self.assert_all_requests_were_expected = assert_all_requests_were_expected
        self.can_send_already_matched_responses = can_send_already_matched_responses
        self.should_mock = should_mock
