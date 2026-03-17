# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._apple_pay_domain import ApplePayDomain
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._apple_pay_domain_create_params import (
        ApplePayDomainCreateParams,
    )
    from stripe.params._apple_pay_domain_delete_params import (
        ApplePayDomainDeleteParams,
    )
    from stripe.params._apple_pay_domain_list_params import (
        ApplePayDomainListParams,
    )
    from stripe.params._apple_pay_domain_retrieve_params import (
        ApplePayDomainRetrieveParams,
    )


class ApplePayDomainService(StripeService):
    def delete(
        self,
        domain: str,
        params: Optional["ApplePayDomainDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            self._request(
                "delete",
                "/v1/apple_pay/domains/{domain}".format(
                    domain=sanitize_id(domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        domain: str,
        params: Optional["ApplePayDomainDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            await self._request_async(
                "delete",
                "/v1/apple_pay/domains/{domain}".format(
                    domain=sanitize_id(domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        domain: str,
        params: Optional["ApplePayDomainRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplePayDomain":
        """
        Retrieve an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            self._request(
                "get",
                "/v1/apple_pay/domains/{domain}".format(
                    domain=sanitize_id(domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        domain: str,
        params: Optional["ApplePayDomainRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplePayDomain":
        """
        Retrieve an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            await self._request_async(
                "get",
                "/v1/apple_pay/domains/{domain}".format(
                    domain=sanitize_id(domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["ApplePayDomainListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ApplePayDomain]":
        """
        List apple pay domains.
        """
        return cast(
            "ListObject[ApplePayDomain]",
            self._request(
                "get",
                "/v1/apple_pay/domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["ApplePayDomainListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ApplePayDomain]":
        """
        List apple pay domains.
        """
        return cast(
            "ListObject[ApplePayDomain]",
            await self._request_async(
                "get",
                "/v1/apple_pay/domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "ApplePayDomainCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ApplePayDomain":
        """
        Create an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            self._request(
                "post",
                "/v1/apple_pay/domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ApplePayDomainCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ApplePayDomain":
        """
        Create an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            await self._request_async(
                "post",
                "/v1/apple_pay/domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )
