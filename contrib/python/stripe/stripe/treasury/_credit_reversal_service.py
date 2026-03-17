# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._credit_reversal_create_params import (
        CreditReversalCreateParams,
    )
    from stripe.params.treasury._credit_reversal_list_params import (
        CreditReversalListParams,
    )
    from stripe.params.treasury._credit_reversal_retrieve_params import (
        CreditReversalRetrieveParams,
    )
    from stripe.treasury._credit_reversal import CreditReversal


class CreditReversalService(StripeService):
    def list(
        self,
        params: "CreditReversalListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditReversal]":
        """
        Returns a list of CreditReversals.
        """
        return cast(
            "ListObject[CreditReversal]",
            self._request(
                "get",
                "/v1/treasury/credit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "CreditReversalListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditReversal]":
        """
        Returns a list of CreditReversals.
        """
        return cast(
            "ListObject[CreditReversal]",
            await self._request_async(
                "get",
                "/v1/treasury/credit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "CreditReversalCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditReversal":
        """
        Reverses a ReceivedCredit and creates a CreditReversal object.
        """
        return cast(
            "CreditReversal",
            self._request(
                "post",
                "/v1/treasury/credit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "CreditReversalCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CreditReversal":
        """
        Reverses a ReceivedCredit and creates a CreditReversal object.
        """
        return cast(
            "CreditReversal",
            await self._request_async(
                "post",
                "/v1/treasury/credit_reversals",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        credit_reversal: str,
        params: Optional["CreditReversalRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditReversal":
        """
        Retrieves the details of an existing CreditReversal by passing the unique CreditReversal ID from either the CreditReversal creation request or CreditReversal list
        """
        return cast(
            "CreditReversal",
            self._request(
                "get",
                "/v1/treasury/credit_reversals/{credit_reversal}".format(
                    credit_reversal=sanitize_id(credit_reversal),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        credit_reversal: str,
        params: Optional["CreditReversalRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CreditReversal":
        """
        Retrieves the details of an existing CreditReversal by passing the unique CreditReversal ID from either the CreditReversal creation request or CreditReversal list
        """
        return cast(
            "CreditReversal",
            await self._request_async(
                "get",
                "/v1/treasury/credit_reversals/{credit_reversal}".format(
                    credit_reversal=sanitize_id(credit_reversal),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
