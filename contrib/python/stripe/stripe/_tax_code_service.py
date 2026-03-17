# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._tax_code import TaxCode
    from stripe.params._tax_code_list_params import TaxCodeListParams
    from stripe.params._tax_code_retrieve_params import TaxCodeRetrieveParams


class TaxCodeService(StripeService):
    def list(
        self,
        params: Optional["TaxCodeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TaxCode]":
        """
        A list of [all tax codes available](https://stripe.com/docs/tax/tax-categories) to add to Products in order to allow specific tax calculations.
        """
        return cast(
            "ListObject[TaxCode]",
            self._request(
                "get",
                "/v1/tax_codes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["TaxCodeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TaxCode]":
        """
        A list of [all tax codes available](https://stripe.com/docs/tax/tax-categories) to add to Products in order to allow specific tax calculations.
        """
        return cast(
            "ListObject[TaxCode]",
            await self._request_async(
                "get",
                "/v1/tax_codes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["TaxCodeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxCode":
        """
        Retrieves the details of an existing tax code. Supply the unique tax code ID and Stripe will return the corresponding tax code information.
        """
        return cast(
            "TaxCode",
            self._request(
                "get",
                "/v1/tax_codes/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["TaxCodeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "TaxCode":
        """
        Retrieves the details of an existing tax code. Supply the unique tax code ID and Stripe will return the corresponding tax code information.
        """
        return cast(
            "TaxCode",
            await self._request_async(
                "get",
                "/v1/tax_codes/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
