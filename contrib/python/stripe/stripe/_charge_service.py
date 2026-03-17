# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._charge import Charge
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe.params._charge_capture_params import ChargeCaptureParams
    from stripe.params._charge_create_params import ChargeCreateParams
    from stripe.params._charge_list_params import ChargeListParams
    from stripe.params._charge_retrieve_params import ChargeRetrieveParams
    from stripe.params._charge_search_params import ChargeSearchParams
    from stripe.params._charge_update_params import ChargeUpdateParams


class ChargeService(StripeService):
    def list(
        self,
        params: Optional["ChargeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Charge]":
        """
        Returns a list of charges you've previously created. The charges are returned in sorted order, with the most recent charges appearing first.
        """
        return cast(
            "ListObject[Charge]",
            self._request(
                "get",
                "/v1/charges",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ChargeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Charge]":
        """
        Returns a list of charges you've previously created. The charges are returned in sorted order, with the most recent charges appearing first.
        """
        return cast(
            "ListObject[Charge]",
            await self._request_async(
                "get",
                "/v1/charges",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["ChargeCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        This method is no longer recommended—use the [Payment Intents API](https://docs.stripe.com/docs/api/payment_intents)
        to initiate a new payment instead. Confirmation of the PaymentIntent creates the Charge
        object used to request payment.
        """
        return cast(
            "Charge",
            self._request(
                "post",
                "/v1/charges",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["ChargeCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        This method is no longer recommended—use the [Payment Intents API](https://docs.stripe.com/docs/api/payment_intents)
        to initiate a new payment instead. Confirmation of the PaymentIntent creates the Charge
        object used to request payment.
        """
        return cast(
            "Charge",
            await self._request_async(
                "post",
                "/v1/charges",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        charge: str,
        params: Optional["ChargeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        Retrieves the details of a charge that has previously been created. Supply the unique charge ID that was returned from your previous request, and Stripe will return the corresponding charge information. The same information is returned when creating or refunding the charge.
        """
        return cast(
            "Charge",
            self._request(
                "get",
                "/v1/charges/{charge}".format(charge=sanitize_id(charge)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        charge: str,
        params: Optional["ChargeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        Retrieves the details of a charge that has previously been created. Supply the unique charge ID that was returned from your previous request, and Stripe will return the corresponding charge information. The same information is returned when creating or refunding the charge.
        """
        return cast(
            "Charge",
            await self._request_async(
                "get",
                "/v1/charges/{charge}".format(charge=sanitize_id(charge)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        charge: str,
        params: Optional["ChargeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        Updates the specified charge by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Charge",
            self._request(
                "post",
                "/v1/charges/{charge}".format(charge=sanitize_id(charge)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        charge: str,
        params: Optional["ChargeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        Updates the specified charge by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Charge",
            await self._request_async(
                "post",
                "/v1/charges/{charge}".format(charge=sanitize_id(charge)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "ChargeSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Charge]":
        """
        Search for charges you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Charge]",
            self._request(
                "get",
                "/v1/charges/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "ChargeSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Charge]":
        """
        Search for charges you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Charge]",
            await self._request_async(
                "get",
                "/v1/charges/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def capture(
        self,
        charge: str,
        params: Optional["ChargeCaptureParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        return cast(
            "Charge",
            self._request(
                "post",
                "/v1/charges/{charge}/capture".format(
                    charge=sanitize_id(charge),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def capture_async(
        self,
        charge: str,
        params: Optional["ChargeCaptureParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        return cast(
            "Charge",
            await self._request_async(
                "post",
                "/v1/charges/{charge}/capture".format(
                    charge=sanitize_id(charge),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
