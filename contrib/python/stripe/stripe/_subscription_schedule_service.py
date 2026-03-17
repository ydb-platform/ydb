# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._subscription_schedule import SubscriptionSchedule
    from stripe.params._subscription_schedule_cancel_params import (
        SubscriptionScheduleCancelParams,
    )
    from stripe.params._subscription_schedule_create_params import (
        SubscriptionScheduleCreateParams,
    )
    from stripe.params._subscription_schedule_list_params import (
        SubscriptionScheduleListParams,
    )
    from stripe.params._subscription_schedule_release_params import (
        SubscriptionScheduleReleaseParams,
    )
    from stripe.params._subscription_schedule_retrieve_params import (
        SubscriptionScheduleRetrieveParams,
    )
    from stripe.params._subscription_schedule_update_params import (
        SubscriptionScheduleUpdateParams,
    )


class SubscriptionScheduleService(StripeService):
    def list(
        self,
        params: Optional["SubscriptionScheduleListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SubscriptionSchedule]":
        """
        Retrieves the list of your subscription schedules.
        """
        return cast(
            "ListObject[SubscriptionSchedule]",
            self._request(
                "get",
                "/v1/subscription_schedules",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["SubscriptionScheduleListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SubscriptionSchedule]":
        """
        Retrieves the list of your subscription schedules.
        """
        return cast(
            "ListObject[SubscriptionSchedule]",
            await self._request_async(
                "get",
                "/v1/subscription_schedules",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["SubscriptionScheduleCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Creates a new subscription schedule object. Each customer can have up to 500 active or scheduled subscriptions.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "post",
                "/v1/subscription_schedules",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["SubscriptionScheduleCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Creates a new subscription schedule object. Each customer can have up to 500 active or scheduled subscriptions.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "post",
                "/v1/subscription_schedules",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Retrieves the details of an existing subscription schedule. You only need to supply the unique subscription schedule identifier that was returned upon subscription schedule creation.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "get",
                "/v1/subscription_schedules/{schedule}".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Retrieves the details of an existing subscription schedule. You only need to supply the unique subscription schedule identifier that was returned upon subscription schedule creation.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "get",
                "/v1/subscription_schedules/{schedule}".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Updates an existing subscription schedule.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "post",
                "/v1/subscription_schedules/{schedule}".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Updates an existing subscription schedule.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "post",
                "/v1/subscription_schedules/{schedule}".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "post",
                "/v1/subscription_schedules/{schedule}/cancel".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Cancels a subscription schedule and its associated subscription immediately (if the subscription schedule has an active subscription). A subscription schedule can only be canceled if its status is not_started or active.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "post",
                "/v1/subscription_schedules/{schedule}/cancel".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def release(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleReleaseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        return cast(
            "SubscriptionSchedule",
            self._request(
                "post",
                "/v1/subscription_schedules/{schedule}/release".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def release_async(
        self,
        schedule: str,
        params: Optional["SubscriptionScheduleReleaseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SubscriptionSchedule":
        """
        Releases the subscription schedule immediately, which will stop scheduling of its phases, but leave any existing subscription in place. A schedule can only be released if its status is not_started or active. If the subscription schedule is currently associated with a subscription, releasing it will remove its subscription property and set the subscription's ID to the released_subscription property.
        """
        return cast(
            "SubscriptionSchedule",
            await self._request_async(
                "post",
                "/v1/subscription_schedules/{schedule}/release".format(
                    schedule=sanitize_id(schedule),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
