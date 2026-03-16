# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_method_domain import PaymentMethodDomain
    from stripe._request_options import RequestOptions
    from stripe.params._payment_method_domain_create_params import (
        PaymentMethodDomainCreateParams,
    )
    from stripe.params._payment_method_domain_list_params import (
        PaymentMethodDomainListParams,
    )
    from stripe.params._payment_method_domain_retrieve_params import (
        PaymentMethodDomainRetrieveParams,
    )
    from stripe.params._payment_method_domain_update_params import (
        PaymentMethodDomainUpdateParams,
    )
    from stripe.params._payment_method_domain_validate_params import (
        PaymentMethodDomainValidateParams,
    )


class PaymentMethodDomainService(StripeService):
    def list(
        self,
        params: Optional["PaymentMethodDomainListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentMethodDomain]":
        """
        Lists the details of existing payment method domains.
        """
        return cast(
            "ListObject[PaymentMethodDomain]",
            self._request(
                "get",
                "/v1/payment_method_domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PaymentMethodDomainListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentMethodDomain]":
        """
        Lists the details of existing payment method domains.
        """
        return cast(
            "ListObject[PaymentMethodDomain]",
            await self._request_async(
                "get",
                "/v1/payment_method_domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PaymentMethodDomainCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Creates a payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            self._request(
                "post",
                "/v1/payment_method_domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PaymentMethodDomainCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Creates a payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            await self._request_async(
                "post",
                "/v1/payment_method_domains",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        payment_method_domain: str,
        params: Optional["PaymentMethodDomainRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Retrieves the details of an existing payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            self._request(
                "get",
                "/v1/payment_method_domains/{payment_method_domain}".format(
                    payment_method_domain=sanitize_id(payment_method_domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        payment_method_domain: str,
        params: Optional["PaymentMethodDomainRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Retrieves the details of an existing payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            await self._request_async(
                "get",
                "/v1/payment_method_domains/{payment_method_domain}".format(
                    payment_method_domain=sanitize_id(payment_method_domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        payment_method_domain: str,
        params: Optional["PaymentMethodDomainUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Updates an existing payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            self._request(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}".format(
                    payment_method_domain=sanitize_id(payment_method_domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        payment_method_domain: str,
        params: Optional["PaymentMethodDomainUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Updates an existing payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            await self._request_async(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}".format(
                    payment_method_domain=sanitize_id(payment_method_domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def validate(
        self,
        payment_method_domain: str,
        params: Optional["PaymentMethodDomainValidateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        return cast(
            "PaymentMethodDomain",
            self._request(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}/validate".format(
                    payment_method_domain=sanitize_id(payment_method_domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def validate_async(
        self,
        payment_method_domain: str,
        params: Optional["PaymentMethodDomainValidateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        return cast(
            "PaymentMethodDomain",
            await self._request_async(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}/validate".format(
                    payment_method_domain=sanitize_id(payment_method_domain),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
