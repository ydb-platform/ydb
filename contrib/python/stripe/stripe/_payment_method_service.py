# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_method import PaymentMethod
    from stripe._request_options import RequestOptions
    from stripe.params._payment_method_attach_params import (
        PaymentMethodAttachParams,
    )
    from stripe.params._payment_method_create_params import (
        PaymentMethodCreateParams,
    )
    from stripe.params._payment_method_detach_params import (
        PaymentMethodDetachParams,
    )
    from stripe.params._payment_method_list_params import (
        PaymentMethodListParams,
    )
    from stripe.params._payment_method_retrieve_params import (
        PaymentMethodRetrieveParams,
    )
    from stripe.params._payment_method_update_params import (
        PaymentMethodUpdateParams,
    )


class PaymentMethodService(StripeService):
    def list(
        self,
        params: Optional["PaymentMethodListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentMethod]":
        """
        Returns a list of all PaymentMethods.
        """
        return cast(
            "ListObject[PaymentMethod]",
            self._request(
                "get",
                "/v1/payment_methods",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PaymentMethodListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentMethod]":
        """
        Returns a list of all PaymentMethods.
        """
        return cast(
            "ListObject[PaymentMethod]",
            await self._request_async(
                "get",
                "/v1/payment_methods",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["PaymentMethodCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Creates a PaymentMethod object. Read the [Stripe.js reference](https://docs.stripe.com/docs/stripe-js/reference#stripe-create-payment-method) to learn how to create PaymentMethods via Stripe.js.

        Instead of creating a PaymentMethod directly, we recommend using the [PaymentIntents API to accept a payment immediately or the <a href="/docs/payments/save-and-reuse">SetupIntent](https://docs.stripe.com/docs/payments/accept-a-payment) API to collect payment method details ahead of a future payment.
        """
        return cast(
            "PaymentMethod",
            self._request(
                "post",
                "/v1/payment_methods",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["PaymentMethodCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Creates a PaymentMethod object. Read the [Stripe.js reference](https://docs.stripe.com/docs/stripe-js/reference#stripe-create-payment-method) to learn how to create PaymentMethods via Stripe.js.

        Instead of creating a PaymentMethod directly, we recommend using the [PaymentIntents API to accept a payment immediately or the <a href="/docs/payments/save-and-reuse">SetupIntent](https://docs.stripe.com/docs/payments/accept-a-payment) API to collect payment method details ahead of a future payment.
        """
        return cast(
            "PaymentMethod",
            await self._request_async(
                "post",
                "/v1/payment_methods",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        payment_method: str,
        params: Optional["PaymentMethodRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object attached to the StripeAccount. To retrieve a payment method attached to a Customer, you should use [Retrieve a Customer's PaymentMethods](https://docs.stripe.com/docs/api/payment_methods/customer)
        """
        return cast(
            "PaymentMethod",
            self._request(
                "get",
                "/v1/payment_methods/{payment_method}".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        payment_method: str,
        params: Optional["PaymentMethodRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Retrieves a PaymentMethod object attached to the StripeAccount. To retrieve a payment method attached to a Customer, you should use [Retrieve a Customer's PaymentMethods](https://docs.stripe.com/docs/api/payment_methods/customer)
        """
        return cast(
            "PaymentMethod",
            await self._request_async(
                "get",
                "/v1/payment_methods/{payment_method}".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        payment_method: str,
        params: Optional["PaymentMethodUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Updates a PaymentMethod object. A PaymentMethod must be attached to a customer to be updated.
        """
        return cast(
            "PaymentMethod",
            self._request(
                "post",
                "/v1/payment_methods/{payment_method}".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        payment_method: str,
        params: Optional["PaymentMethodUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Updates a PaymentMethod object. A PaymentMethod must be attached to a customer to be updated.
        """
        return cast(
            "PaymentMethod",
            await self._request_async(
                "post",
                "/v1/payment_methods/{payment_method}".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def attach(
        self,
        payment_method: str,
        params: Optional["PaymentMethodAttachParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Attaches a PaymentMethod object to a Customer.

        To attach a new PaymentMethod to a customer for future payments, we recommend you use a [SetupIntent](https://docs.stripe.com/docs/api/setup_intents)
        or a PaymentIntent with [setup_future_usage](https://docs.stripe.com/docs/api/payment_intents/create#create_payment_intent-setup_future_usage).
        These approaches will perform any necessary steps to set up the PaymentMethod for future payments. Using the /v1/payment_methods/:id/attach
        endpoint without first using a SetupIntent or PaymentIntent with setup_future_usage does not optimize the PaymentMethod for
        future use, which makes later declines and payment friction more likely.
        See [Optimizing cards for future payments](https://docs.stripe.com/docs/payments/payment-intents#future-usage) for more information about setting up
        future payments.

        To use this PaymentMethod as the default for invoice or subscription payments,
        set [invoice_settings.default_payment_method](https://docs.stripe.com/docs/api/customers/update#update_customer-invoice_settings-default_payment_method),
        on the Customer to the PaymentMethod's ID.
        """
        return cast(
            "PaymentMethod",
            self._request(
                "post",
                "/v1/payment_methods/{payment_method}/attach".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def attach_async(
        self,
        payment_method: str,
        params: Optional["PaymentMethodAttachParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Attaches a PaymentMethod object to a Customer.

        To attach a new PaymentMethod to a customer for future payments, we recommend you use a [SetupIntent](https://docs.stripe.com/docs/api/setup_intents)
        or a PaymentIntent with [setup_future_usage](https://docs.stripe.com/docs/api/payment_intents/create#create_payment_intent-setup_future_usage).
        These approaches will perform any necessary steps to set up the PaymentMethod for future payments. Using the /v1/payment_methods/:id/attach
        endpoint without first using a SetupIntent or PaymentIntent with setup_future_usage does not optimize the PaymentMethod for
        future use, which makes later declines and payment friction more likely.
        See [Optimizing cards for future payments](https://docs.stripe.com/docs/payments/payment-intents#future-usage) for more information about setting up
        future payments.

        To use this PaymentMethod as the default for invoice or subscription payments,
        set [invoice_settings.default_payment_method](https://docs.stripe.com/docs/api/customers/update#update_customer-invoice_settings-default_payment_method),
        on the Customer to the PaymentMethod's ID.
        """
        return cast(
            "PaymentMethod",
            await self._request_async(
                "post",
                "/v1/payment_methods/{payment_method}/attach".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def detach(
        self,
        payment_method: str,
        params: Optional["PaymentMethodDetachParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Detaches a PaymentMethod object from a Customer. After a PaymentMethod is detached, it can no longer be used for a payment or re-attached to a Customer.
        """
        return cast(
            "PaymentMethod",
            self._request(
                "post",
                "/v1/payment_methods/{payment_method}/detach".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def detach_async(
        self,
        payment_method: str,
        params: Optional["PaymentMethodDetachParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentMethod":
        """
        Detaches a PaymentMethod object from a Customer. After a PaymentMethod is detached, it can no longer be used for a payment or re-attached to a Customer.
        """
        return cast(
            "PaymentMethod",
            await self._request_async(
                "post",
                "/v1/payment_methods/{payment_method}/detach".format(
                    payment_method=sanitize_id(payment_method),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
