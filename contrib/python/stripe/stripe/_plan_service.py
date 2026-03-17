# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._plan import Plan
    from stripe._request_options import RequestOptions
    from stripe.params._plan_create_params import PlanCreateParams
    from stripe.params._plan_delete_params import PlanDeleteParams
    from stripe.params._plan_list_params import PlanListParams
    from stripe.params._plan_retrieve_params import PlanRetrieveParams
    from stripe.params._plan_update_params import PlanUpdateParams


class PlanService(StripeService):
    def delete(
        self,
        plan: str,
        params: Optional["PlanDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        return cast(
            "Plan",
            self._request(
                "delete",
                "/v1/plans/{plan}".format(plan=sanitize_id(plan)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        plan: str,
        params: Optional["PlanDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        Deleting plans means new subscribers can't be added. Existing subscribers aren't affected.
        """
        return cast(
            "Plan",
            await self._request_async(
                "delete",
                "/v1/plans/{plan}".format(plan=sanitize_id(plan)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        plan: str,
        params: Optional["PlanRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        Retrieves the plan with the given ID.
        """
        return cast(
            "Plan",
            self._request(
                "get",
                "/v1/plans/{plan}".format(plan=sanitize_id(plan)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        plan: str,
        params: Optional["PlanRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        Retrieves the plan with the given ID.
        """
        return cast(
            "Plan",
            await self._request_async(
                "get",
                "/v1/plans/{plan}".format(plan=sanitize_id(plan)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        plan: str,
        params: Optional["PlanUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        Updates the specified plan by setting the values of the parameters passed. Any parameters not provided are left unchanged. By design, you cannot change a plan's ID, amount, currency, or billing cycle.
        """
        return cast(
            "Plan",
            self._request(
                "post",
                "/v1/plans/{plan}".format(plan=sanitize_id(plan)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        plan: str,
        params: Optional["PlanUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        Updates the specified plan by setting the values of the parameters passed. Any parameters not provided are left unchanged. By design, you cannot change a plan's ID, amount, currency, or billing cycle.
        """
        return cast(
            "Plan",
            await self._request_async(
                "post",
                "/v1/plans/{plan}".format(plan=sanitize_id(plan)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["PlanListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Plan]":
        """
        Returns a list of your plans.
        """
        return cast(
            "ListObject[Plan]",
            self._request(
                "get",
                "/v1/plans",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PlanListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Plan]":
        """
        Returns a list of your plans.
        """
        return cast(
            "ListObject[Plan]",
            await self._request_async(
                "get",
                "/v1/plans",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PlanCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        You can now model subscriptions more flexibly using the [Prices API](https://docs.stripe.com/api#prices). It replaces the Plans API and is backwards compatible to simplify your migration.
        """
        return cast(
            "Plan",
            self._request(
                "post",
                "/v1/plans",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PlanCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Plan":
        """
        You can now model subscriptions more flexibly using the [Prices API](https://docs.stripe.com/api#prices). It replaces the Plans API and is backwards compatible to simplify your migration.
        """
        return cast(
            "Plan",
            await self._request_async(
                "post",
                "/v1/plans",
                base_address="api",
                params=params,
                options=options,
            ),
        )
