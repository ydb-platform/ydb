# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._transfer import Transfer
    from stripe._transfer_reversal_service import TransferReversalService
    from stripe.params._transfer_create_params import TransferCreateParams
    from stripe.params._transfer_list_params import TransferListParams
    from stripe.params._transfer_retrieve_params import TransferRetrieveParams
    from stripe.params._transfer_update_params import TransferUpdateParams

_subservices = {
    "reversals": [
        "stripe._transfer_reversal_service",
        "TransferReversalService",
    ],
}


class TransferService(StripeService):
    reversals: "TransferReversalService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()

    def list(
        self,
        params: Optional["TransferListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Transfer]":
        """
        Returns a list of existing transfers sent to connected accounts. The transfers are returned in sorted order, with the most recently created transfers appearing first.
        """
        return cast(
            "ListObject[Transfer]",
            self._request(
                "get",
                "/v1/transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["TransferListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Transfer]":
        """
        Returns a list of existing transfers sent to connected accounts. The transfers are returned in sorted order, with the most recently created transfers appearing first.
        """
        return cast(
            "ListObject[Transfer]",
            await self._request_async(
                "get",
                "/v1/transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "TransferCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Transfer":
        """
        To send funds from your Stripe account to a connected account, you create a new transfer object. Your [Stripe balance](https://docs.stripe.com/api#balance) must be able to cover the transfer amount, or you'll receive an “Insufficient Funds” error.
        """
        return cast(
            "Transfer",
            self._request(
                "post",
                "/v1/transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "TransferCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Transfer":
        """
        To send funds from your Stripe account to a connected account, you create a new transfer object. Your [Stripe balance](https://docs.stripe.com/api#balance) must be able to cover the transfer amount, or you'll receive an “Insufficient Funds” error.
        """
        return cast(
            "Transfer",
            await self._request_async(
                "post",
                "/v1/transfers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        transfer: str,
        params: Optional["TransferRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transfer":
        """
        Retrieves the details of an existing transfer. Supply the unique transfer ID from either a transfer creation request or the transfer list, and Stripe will return the corresponding transfer information.
        """
        return cast(
            "Transfer",
            self._request(
                "get",
                "/v1/transfers/{transfer}".format(
                    transfer=sanitize_id(transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        transfer: str,
        params: Optional["TransferRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transfer":
        """
        Retrieves the details of an existing transfer. Supply the unique transfer ID from either a transfer creation request or the transfer list, and Stripe will return the corresponding transfer information.
        """
        return cast(
            "Transfer",
            await self._request_async(
                "get",
                "/v1/transfers/{transfer}".format(
                    transfer=sanitize_id(transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        transfer: str,
        params: Optional["TransferUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transfer":
        """
        Updates the specified transfer by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request accepts only metadata as an argument.
        """
        return cast(
            "Transfer",
            self._request(
                "post",
                "/v1/transfers/{transfer}".format(
                    transfer=sanitize_id(transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        transfer: str,
        params: Optional["TransferUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Transfer":
        """
        Updates the specified transfer by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request accepts only metadata as an argument.
        """
        return cast(
            "Transfer",
            await self._request_async(
                "post",
                "/v1/transfers/{transfer}".format(
                    transfer=sanitize_id(transfer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
