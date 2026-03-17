# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._dispute import Dispute
    from stripe.params.issuing._dispute_create_params import (
        DisputeCreateParams,
    )
    from stripe.params.issuing._dispute_list_params import DisputeListParams
    from stripe.params.issuing._dispute_retrieve_params import (
        DisputeRetrieveParams,
    )
    from stripe.params.issuing._dispute_submit_params import (
        DisputeSubmitParams,
    )
    from stripe.params.issuing._dispute_update_params import (
        DisputeUpdateParams,
    )


class DisputeService(StripeService):
    def list(
        self,
        params: Optional["DisputeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Dispute]":
        """
        Returns a list of Issuing Dispute objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Dispute]",
            self._request(
                "get",
                "/v1/issuing/disputes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["DisputeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Dispute]":
        """
        Returns a list of Issuing Dispute objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Dispute]",
            await self._request_async(
                "get",
                "/v1/issuing/disputes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["DisputeCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Creates an Issuing Dispute object. Individual pieces of evidence within the evidence object are optional at this point. Stripe only validates that required evidence is present during submission. Refer to [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence) for more details about evidence requirements.
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/issuing/disputes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["DisputeCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Creates an Issuing Dispute object. Individual pieces of evidence within the evidence object are optional at this point. Stripe only validates that required evidence is present during submission. Refer to [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence) for more details about evidence requirements.
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/issuing/disputes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        dispute: str,
        params: Optional["DisputeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Retrieves an Issuing Dispute object.
        """
        return cast(
            "Dispute",
            self._request(
                "get",
                "/v1/issuing/disputes/{dispute}".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        dispute: str,
        params: Optional["DisputeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Retrieves an Issuing Dispute object.
        """
        return cast(
            "Dispute",
            await self._request_async(
                "get",
                "/v1/issuing/disputes/{dispute}".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        dispute: str,
        params: Optional["DisputeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Updates the specified Issuing Dispute object by setting the values of the parameters passed. Any parameters not provided will be left unchanged. Properties on the evidence object can be unset by passing in an empty string.
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/issuing/disputes/{dispute}".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        dispute: str,
        params: Optional["DisputeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Updates the specified Issuing Dispute object by setting the values of the parameters passed. Any parameters not provided will be left unchanged. Properties on the evidence object can be unset by passing in an empty string.
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/issuing/disputes/{dispute}".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def submit(
        self,
        dispute: str,
        params: Optional["DisputeSubmitParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/issuing/disputes/{dispute}/submit".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def submit_async(
        self,
        dispute: str,
        params: Optional["DisputeSubmitParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Submits an Issuing Dispute to the card network. Stripe validates that all evidence fields required for the dispute's reason are present. For more details, see [Dispute reasons and evidence](https://docs.stripe.com/docs/issuing/purchases/disputes#dispute-reasons-and-evidence).
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/issuing/disputes/{dispute}/submit".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
