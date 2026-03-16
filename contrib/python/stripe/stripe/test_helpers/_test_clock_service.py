# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers._test_clock_advance_params import (
        TestClockAdvanceParams,
    )
    from stripe.params.test_helpers._test_clock_create_params import (
        TestClockCreateParams,
    )
    from stripe.params.test_helpers._test_clock_delete_params import (
        TestClockDeleteParams,
    )
    from stripe.params.test_helpers._test_clock_list_params import (
        TestClockListParams,
    )
    from stripe.params.test_helpers._test_clock_retrieve_params import (
        TestClockRetrieveParams,
    )
    from stripe.test_helpers._test_clock import TestClock


class TestClockService(StripeService):
    def delete(
        self,
        test_clock: str,
        params: Optional["TestClockDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        return cast(
            "TestClock",
            self._request(
                "delete",
                "/v1/test_helpers/test_clocks/{test_clock}".format(
                    test_clock=sanitize_id(test_clock),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        test_clock: str,
        params: Optional["TestClockDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        return cast(
            "TestClock",
            await self._request_async(
                "delete",
                "/v1/test_helpers/test_clocks/{test_clock}".format(
                    test_clock=sanitize_id(test_clock),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        test_clock: str,
        params: Optional["TestClockRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Retrieves a test clock.
        """
        return cast(
            "TestClock",
            self._request(
                "get",
                "/v1/test_helpers/test_clocks/{test_clock}".format(
                    test_clock=sanitize_id(test_clock),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        test_clock: str,
        params: Optional["TestClockRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Retrieves a test clock.
        """
        return cast(
            "TestClock",
            await self._request_async(
                "get",
                "/v1/test_helpers/test_clocks/{test_clock}".format(
                    test_clock=sanitize_id(test_clock),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["TestClockListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TestClock]":
        """
        Returns a list of your test clocks.
        """
        return cast(
            "ListObject[TestClock]",
            self._request(
                "get",
                "/v1/test_helpers/test_clocks",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["TestClockListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TestClock]":
        """
        Returns a list of your test clocks.
        """
        return cast(
            "ListObject[TestClock]",
            await self._request_async(
                "get",
                "/v1/test_helpers/test_clocks",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "TestClockCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Creates a new test clock that can be attached to new customers and quotes.
        """
        return cast(
            "TestClock",
            self._request(
                "post",
                "/v1/test_helpers/test_clocks",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "TestClockCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Creates a new test clock that can be attached to new customers and quotes.
        """
        return cast(
            "TestClock",
            await self._request_async(
                "post",
                "/v1/test_helpers/test_clocks",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def advance(
        self,
        test_clock: str,
        params: "TestClockAdvanceParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        return cast(
            "TestClock",
            self._request(
                "post",
                "/v1/test_helpers/test_clocks/{test_clock}/advance".format(
                    test_clock=sanitize_id(test_clock),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def advance_async(
        self,
        test_clock: str,
        params: "TestClockAdvanceParams",
        options: Optional["RequestOptions"] = None,
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        return cast(
            "TestClock",
            await self._request_async(
                "post",
                "/v1/test_helpers/test_clocks/{test_clock}/advance".format(
                    test_clock=sanitize_id(test_clock),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
