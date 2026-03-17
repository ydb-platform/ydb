# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
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


class TestClock(
    CreateableAPIResource["TestClock"],
    DeletableAPIResource["TestClock"],
    ListableAPIResource["TestClock"],
):
    """
    A test clock enables deterministic control over objects in testmode. With a test clock, you can create
    objects at a frozen time in the past or future, and advance to a specific future time to observe webhooks and state changes. After the clock advances,
    you can either validate the current state of your scenario (and test your assumptions), change the current state of your scenario (and test more complex scenarios), or keep advancing forward in time.
    """

    OBJECT_NAME: ClassVar[Literal["test_helpers.test_clock"]] = (
        "test_helpers.test_clock"
    )

    class StatusDetails(StripeObject):
        class Advancing(StripeObject):
            target_frozen_time: int
            """
            The `frozen_time` that the Test Clock is advancing towards.
            """

        advancing: Optional[Advancing]
        _inner_class_types = {"advancing": Advancing}

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    deletes_after: int
    """
    Time at which this clock is scheduled to auto delete.
    """
    frozen_time: int
    """
    Time at which all objects belonging to this clock are frozen.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    name: Optional[str]
    """
    The custom name supplied at creation.
    """
    object: Literal["test_helpers.test_clock"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Literal["advancing", "internal_failure", "ready"]
    """
    The status of the Test Clock.
    """
    status_details: StatusDetails

    @classmethod
    def _cls_advance(
        cls, test_clock: str, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        return cast(
            "TestClock",
            cls._static_request(
                "post",
                "/v1/test_helpers/test_clocks/{test_clock}/advance".format(
                    test_clock=sanitize_id(test_clock)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def advance(
        test_clock: str, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        ...

    @overload
    def advance(
        self, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        ...

    @class_method_variant("_cls_advance")
    def advance(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        return cast(
            "TestClock",
            self._request(
                "post",
                "/v1/test_helpers/test_clocks/{test_clock}/advance".format(
                    test_clock=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_advance_async(
        cls, test_clock: str, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        return cast(
            "TestClock",
            await cls._static_request_async(
                "post",
                "/v1/test_helpers/test_clocks/{test_clock}/advance".format(
                    test_clock=sanitize_id(test_clock)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def advance_async(
        test_clock: str, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        ...

    @overload
    async def advance_async(
        self, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        ...

    @class_method_variant("_cls_advance_async")
    async def advance_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TestClockAdvanceParams"]
    ) -> "TestClock":
        """
        Starts advancing a test clock to a specified time in the future. Advancement is done when status changes to Ready.
        """
        return cast(
            "TestClock",
            await self._request_async(
                "post",
                "/v1/test_helpers/test_clocks/{test_clock}/advance".format(
                    test_clock=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["TestClockCreateParams"]) -> "TestClock":
        """
        Creates a new test clock that can be attached to new customers and quotes.
        """
        return cast(
            "TestClock",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["TestClockCreateParams"]
    ) -> "TestClock":
        """
        Creates a new test clock that can be attached to new customers and quotes.
        """
        return cast(
            "TestClock",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "TestClock",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        ...

    @overload
    def delete(self, **params: Unpack["TestClockDeleteParams"]) -> "TestClock":
        """
        Deletes a test clock.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "TestClock",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["TestClockDeleteParams"]
    ) -> "TestClock":
        """
        Deletes a test clock.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["TestClockListParams"]
    ) -> ListObject["TestClock"]:
        """
        Returns a list of your test clocks.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["TestClockListParams"]
    ) -> ListObject["TestClock"]:
        """
        Returns a list of your test clocks.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["TestClockRetrieveParams"]
    ) -> "TestClock":
        """
        Retrieves a test clock.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TestClockRetrieveParams"]
    ) -> "TestClock":
        """
        Retrieves a test clock.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"status_details": StatusDetails}
