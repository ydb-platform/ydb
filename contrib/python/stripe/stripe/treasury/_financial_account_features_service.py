# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.treasury._financial_account_features_retrieve_params import (
        FinancialAccountFeaturesRetrieveParams,
    )
    from stripe.params.treasury._financial_account_features_update_params import (
        FinancialAccountFeaturesUpdateParams,
    )
    from stripe.treasury._financial_account_features import (
        FinancialAccountFeatures,
    )


class FinancialAccountFeaturesService(StripeService):
    def update(
        self,
        financial_account: str,
        params: Optional["FinancialAccountFeaturesUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            self._request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        financial_account: str,
        params: Optional["FinancialAccountFeaturesUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccountFeatures":
        """
        Updates the Features associated with a FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            await self._request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        financial_account: str,
        params: Optional["FinancialAccountFeaturesRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            self._request(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        financial_account: str,
        params: Optional["FinancialAccountFeaturesRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccountFeatures":
        """
        Retrieves Features information associated with the FinancialAccount.
        """
        return cast(
            "FinancialAccountFeatures",
            await self._request_async(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}/features".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
