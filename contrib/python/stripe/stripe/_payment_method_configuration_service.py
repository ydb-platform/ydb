# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_method_configuration import PaymentMethodConfiguration
    from stripe._request_options import RequestOptions
    from stripe.params._payment_method_configuration_create_params import (
        PaymentMethodConfigurationCreateParams,
    )
    from stripe.params._payment_method_configuration_list_params import (
        PaymentMethodConfigurationListParams,
    )
    from stripe.params._payment_method_configuration_retrieve_params import (
        PaymentMethodConfigurationRetrieveParams,
    )
    from stripe.params._payment_method_configuration_update_params import (
        PaymentMethodConfigurationUpdateParams,
    )


class PaymentMethodConfigurationService(StripeService):
    def list(
        self,
        params: Optional["PaymentMethodConfigurationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentMethodConfiguration]":
        """
        List payment method configurations
        """
        return cast(
            "ListObject[PaymentMethodConfiguration]",
            self._request(
                "get",
                "/v1/payment_method_configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PaymentMethodConfigurationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentMethodConfiguration]":
        """
        List payment method configurations
        """
        return cast(
            "ListObject[PaymentMethodConfiguration]",
            await self._request_async(
                "get",
                "/v1/payment_method_configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["PaymentMethodConfigurationCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodConfiguration":
        """
        Creates a payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            self._request(
                "post",
                "/v1/payment_method_configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["PaymentMethodConfigurationCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodConfiguration":
        """
        Creates a payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            await self._request_async(
                "post",
                "/v1/payment_method_configurations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        configuration: str,
        params: Optional["PaymentMethodConfigurationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodConfiguration":
        """
        Retrieve payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            self._request(
                "get",
                "/v1/payment_method_configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        configuration: str,
        params: Optional["PaymentMethodConfigurationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodConfiguration":
        """
        Retrieve payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            await self._request_async(
                "get",
                "/v1/payment_method_configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        configuration: str,
        params: Optional["PaymentMethodConfigurationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodConfiguration":
        """
        Update payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            self._request(
                "post",
                "/v1/payment_method_configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        configuration: str,
        params: Optional["PaymentMethodConfigurationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethodConfiguration":
        """
        Update payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            await self._request_async(
                "post",
                "/v1/payment_method_configurations/{configuration}".format(
                    configuration=sanitize_id(configuration),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
