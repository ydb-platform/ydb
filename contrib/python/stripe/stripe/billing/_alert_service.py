# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.billing._alert import Alert
    from stripe.params.billing._alert_activate_params import (
        AlertActivateParams,
    )
    from stripe.params.billing._alert_archive_params import AlertArchiveParams
    from stripe.params.billing._alert_create_params import AlertCreateParams
    from stripe.params.billing._alert_deactivate_params import (
        AlertDeactivateParams,
    )
    from stripe.params.billing._alert_list_params import AlertListParams
    from stripe.params.billing._alert_retrieve_params import (
        AlertRetrieveParams,
    )


class AlertService(StripeService):
    def list(
        self,
        params: Optional["AlertListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Alert]":
        """
        Lists billing active and inactive alerts
        """
        return cast(
            "ListObject[Alert]",
            self._request(
                "get",
                "/v1/billing/alerts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["AlertListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Alert]":
        """
        Lists billing active and inactive alerts
        """
        return cast(
            "ListObject[Alert]",
            await self._request_async(
                "get",
                "/v1/billing/alerts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "AlertCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Creates a billing alert
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "AlertCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Creates a billing alert
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["AlertRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Retrieves a billing alert given an ID
        """
        return cast(
            "Alert",
            self._request(
                "get",
                "/v1/billing/alerts/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["AlertRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Retrieves a billing alert given an ID
        """
        return cast(
            "Alert",
            await self._request_async(
                "get",
                "/v1/billing/alerts/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def activate(
        self,
        id: str,
        params: Optional["AlertActivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts/{id}/activate".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def activate_async(
        self,
        id: str,
        params: Optional["AlertActivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts/{id}/activate".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def archive(
        self,
        id: str,
        params: Optional["AlertArchiveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts/{id}/archive".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def archive_async(
        self,
        id: str,
        params: Optional["AlertArchiveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts/{id}/archive".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def deactivate(
        self,
        id: str,
        params: Optional["AlertDeactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def deactivate_async(
        self,
        id: str,
        params: Optional["AlertDeactivateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
