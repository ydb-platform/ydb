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
    from stripe.params.treasury._financial_account_close_params import (
        FinancialAccountCloseParams,
    )
    from stripe.params.treasury._financial_account_create_params import (
        FinancialAccountCreateParams,
    )
    from stripe.params.treasury._financial_account_list_params import (
        FinancialAccountListParams,
    )
    from stripe.params.treasury._financial_account_retrieve_params import (
        FinancialAccountRetrieveParams,
    )
    from stripe.params.treasury._financial_account_update_params import (
        FinancialAccountUpdateParams,
    )
    from stripe.treasury._financial_account import FinancialAccount
    from stripe.treasury._financial_account_features_service import (
        FinancialAccountFeaturesService,
    )

_subservices = {
    "features": [
        "stripe.treasury._financial_account_features_service",
        "FinancialAccountFeaturesService",
    ],
}


class FinancialAccountService(StripeService):
    features: "FinancialAccountFeaturesService"

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
        params: Optional["FinancialAccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[FinancialAccount]":
        """
        Returns a list of FinancialAccounts.
        """
        return cast(
            "ListObject[FinancialAccount]",
            self._request(
                "get",
                "/v1/treasury/financial_accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["FinancialAccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[FinancialAccount]":
        """
        Returns a list of FinancialAccounts.
        """
        return cast(
            "ListObject[FinancialAccount]",
            await self._request_async(
                "get",
                "/v1/treasury/financial_accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "FinancialAccountCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Creates a new FinancialAccount. Each connected account can have up to three FinancialAccounts by default.
        """
        return cast(
            "FinancialAccount",
            self._request(
                "post",
                "/v1/treasury/financial_accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "FinancialAccountCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Creates a new FinancialAccount. Each connected account can have up to three FinancialAccounts by default.
        """
        return cast(
            "FinancialAccount",
            await self._request_async(
                "post",
                "/v1/treasury/financial_accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        financial_account: str,
        params: Optional["FinancialAccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Retrieves the details of a FinancialAccount.
        """
        return cast(
            "FinancialAccount",
            self._request(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}".format(
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
        params: Optional["FinancialAccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Retrieves the details of a FinancialAccount.
        """
        return cast(
            "FinancialAccount",
            await self._request_async(
                "get",
                "/v1/treasury/financial_accounts/{financial_account}".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        financial_account: str,
        params: Optional["FinancialAccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Updates the details of a FinancialAccount.
        """
        return cast(
            "FinancialAccount",
            self._request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}".format(
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
        params: Optional["FinancialAccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Updates the details of a FinancialAccount.
        """
        return cast(
            "FinancialAccount",
            await self._request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def close(
        self,
        financial_account: str,
        params: Optional["FinancialAccountCloseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        return cast(
            "FinancialAccount",
            self._request(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/close".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def close_async(
        self,
        financial_account: str,
        params: Optional["FinancialAccountCloseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "FinancialAccount":
        """
        Closes a FinancialAccount. A FinancialAccount can only be closed if it has a zero balance, has no pending InboundTransfers, and has canceled all attached Issuing cards.
        """
        return cast(
            "FinancialAccount",
            await self._request_async(
                "post",
                "/v1/treasury/financial_accounts/{financial_account}/close".format(
                    financial_account=sanitize_id(financial_account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
