# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_intent import PaymentIntent
    from stripe._payment_intent_amount_details_line_item_service import (
        PaymentIntentAmountDetailsLineItemService,
    )
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe.params._payment_intent_apply_customer_balance_params import (
        PaymentIntentApplyCustomerBalanceParams,
    )
    from stripe.params._payment_intent_cancel_params import (
        PaymentIntentCancelParams,
    )
    from stripe.params._payment_intent_capture_params import (
        PaymentIntentCaptureParams,
    )
    from stripe.params._payment_intent_confirm_params import (
        PaymentIntentConfirmParams,
    )
    from stripe.params._payment_intent_create_params import (
        PaymentIntentCreateParams,
    )
    from stripe.params._payment_intent_increment_authorization_params import (
        PaymentIntentIncrementAuthorizationParams,
    )
    from stripe.params._payment_intent_list_params import (
        PaymentIntentListParams,
    )
    from stripe.params._payment_intent_retrieve_params import (
        PaymentIntentRetrieveParams,
    )
    from stripe.params._payment_intent_search_params import (
        PaymentIntentSearchParams,
    )
    from stripe.params._payment_intent_update_params import (
        PaymentIntentUpdateParams,
    )
    from stripe.params._payment_intent_verify_microdeposits_params import (
        PaymentIntentVerifyMicrodepositsParams,
    )

_subservices = {
    "amount_details_line_items": [
        "stripe._payment_intent_amount_details_line_item_service",
        "PaymentIntentAmountDetailsLineItemService",
    ],
}


class PaymentIntentService(StripeService):
    amount_details_line_items: "PaymentIntentAmountDetailsLineItemService"

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
        params: Optional["PaymentIntentListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentIntent]":
        """
        Returns a list of PaymentIntents.
        """
        return cast(
            "ListObject[PaymentIntent]",
            self._request(
                "get",
                "/v1/payment_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PaymentIntentListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentIntent]":
        """
        Returns a list of PaymentIntents.
        """
        return cast(
            "ListObject[PaymentIntent]",
            await self._request_async(
                "get",
                "/v1/payment_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PaymentIntentCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Creates a PaymentIntent object.

        After the PaymentIntent is created, attach a payment method and [confirm](https://docs.stripe.com/docs/api/payment_intents/confirm)
        to continue the payment. Learn more about <a href="/docs/payments/payment-intents">the available payment flows
        with the Payment Intents API.

        When you use confirm=true during creation, it's equivalent to creating
        and confirming the PaymentIntent in the same call. You can use any parameters
        available in the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) when you supply
        confirm=true.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PaymentIntentCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Creates a PaymentIntent object.

        After the PaymentIntent is created, attach a payment method and [confirm](https://docs.stripe.com/docs/api/payment_intents/confirm)
        to continue the payment. Learn more about <a href="/docs/payments/payment-intents">the available payment flows
        with the Payment Intents API.

        When you use confirm=true during creation, it's equivalent to creating
        and confirming the PaymentIntent in the same call. You can use any parameters
        available in the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) when you supply
        confirm=true.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        intent: str,
        params: Optional["PaymentIntentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Retrieves the details of a PaymentIntent that has previously been created.

        You can retrieve a PaymentIntent client-side using a publishable key when the client_secret is in the query string.

        If you retrieve a PaymentIntent with a publishable key, it only returns a subset of properties. Refer to the [payment intent](https://docs.stripe.com/api#payment_intent_object) object reference for more details.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "get",
                "/v1/payment_intents/{intent}".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        intent: str,
        params: Optional["PaymentIntentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Retrieves the details of a PaymentIntent that has previously been created.

        You can retrieve a PaymentIntent client-side using a publishable key when the client_secret is in the query string.

        If you retrieve a PaymentIntent with a publishable key, it only returns a subset of properties. Refer to the [payment intent](https://docs.stripe.com/api#payment_intent_object) object reference for more details.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "get",
                "/v1/payment_intents/{intent}".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        intent: str,
        params: Optional["PaymentIntentUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Updates properties on a PaymentIntent object without confirming.

        Depending on which properties you update, you might need to confirm the
        PaymentIntent again. For example, updating the payment_method
        always requires you to confirm the PaymentIntent again. If you prefer to
        update and confirm at the same time, we recommend updating properties through
        the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) instead.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        intent: str,
        params: Optional["PaymentIntentUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Updates properties on a PaymentIntent object without confirming.

        Depending on which properties you update, you might need to confirm the
        PaymentIntent again. For example, updating the payment_method
        always requires you to confirm the PaymentIntent again. If you prefer to
        update and confirm at the same time, we recommend updating properties through
        the [confirm API](https://docs.stripe.com/docs/api/payment_intents/confirm) instead.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "PaymentIntentSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[PaymentIntent]":
        """
        Search for PaymentIntents you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[PaymentIntent]",
            self._request(
                "get",
                "/v1/payment_intents/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "PaymentIntentSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[PaymentIntent]":
        """
        Search for PaymentIntents you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[PaymentIntent]",
            await self._request_async(
                "get",
                "/v1/payment_intents/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def apply_customer_balance(
        self,
        intent: str,
        params: Optional["PaymentIntentApplyCustomerBalanceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/apply_customer_balance".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def apply_customer_balance_async(
        self,
        intent: str,
        params: Optional["PaymentIntentApplyCustomerBalanceParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Manually reconcile the remaining amount for a customer_balance PaymentIntent.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/apply_customer_balance".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        intent: str,
        params: Optional["PaymentIntentCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/cancel".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        intent: str,
        params: Optional["PaymentIntentCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        You can cancel a PaymentIntent object when it's in one of these statuses: requires_payment_method, requires_capture, requires_confirmation, requires_action or, [in rare cases](https://docs.stripe.com/docs/payments/intents), processing.

        After it's canceled, no additional charges are made by the PaymentIntent and any operations on the PaymentIntent fail with an error. For PaymentIntents with a status of requires_capture, the remaining amount_capturable is automatically refunded.

        You can't cancel the PaymentIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/cancel".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def capture(
        self,
        intent: str,
        params: Optional["PaymentIntentCaptureParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/capture".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def capture_async(
        self,
        intent: str,
        params: Optional["PaymentIntentCaptureParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Capture the funds of an existing uncaptured PaymentIntent when its status is requires_capture.

        Uncaptured PaymentIntents are cancelled a set number of days (7 by default) after their creation.

        Learn more about [separate authorization and capture](https://docs.stripe.com/docs/payments/capture-later).
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/capture".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def confirm(
        self,
        intent: str,
        params: Optional["PaymentIntentConfirmParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/confirm".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def confirm_async(
        self,
        intent: str,
        params: Optional["PaymentIntentConfirmParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Confirm that your customer intends to pay with current or provided
        payment method. Upon confirmation, the PaymentIntent will attempt to initiate
        a payment.

        If the selected payment method requires additional authentication steps, the
        PaymentIntent will transition to the requires_action status and
        suggest additional actions via next_action. If payment fails,
        the PaymentIntent transitions to the requires_payment_method status or the
        canceled status if the confirmation limit is reached. If
        payment succeeds, the PaymentIntent will transition to the succeeded
        status (or requires_capture, if capture_method is set to manual).

        If the confirmation_method is automatic, payment may be attempted
        using our [client SDKs](https://docs.stripe.com/docs/stripe-js/reference#stripe-handle-card-payment)
        and the PaymentIntent's [client_secret](https://docs.stripe.com/api#payment_intent_object-client_secret).
        After next_actions are handled by the client, no additional
        confirmation is required to complete the payment.

        If the confirmation_method is manual, all payment attempts must be
        initiated using a secret key.

        If any actions are required for the payment, the PaymentIntent will
        return to the requires_confirmation state
        after those actions are completed. Your server needs to then
        explicitly re-confirm the PaymentIntent to initiate the next payment
        attempt.

        There is a variable upper limit on how many times a PaymentIntent can be confirmed.
        After this limit is reached, any further calls to this endpoint will
        transition the PaymentIntent to the canceled state.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/confirm".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def increment_authorization(
        self,
        intent: str,
        params: "PaymentIntentIncrementAuthorizationParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/increment_authorization".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def increment_authorization_async(
        self,
        intent: str,
        params: "PaymentIntentIncrementAuthorizationParams",
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Perform an incremental authorization on an eligible
        [PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/object). To be eligible, the
        PaymentIntent's status must be requires_capture and
        [incremental_authorization_supported](https://docs.stripe.com/docs/api/charges/object#charge_object-payment_method_details-card_present-incremental_authorization_supported)
        must be true.

        Incremental authorizations attempt to increase the authorized amount on
        your customer's card to the new, higher amount provided. Similar to the
        initial authorization, incremental authorizations can be declined. A
        single PaymentIntent can call this endpoint multiple times to further
        increase the authorized amount.

        If the incremental authorization succeeds, the PaymentIntent object
        returns with the updated
        [amount](https://docs.stripe.com/docs/api/payment_intents/object#payment_intent_object-amount).
        If the incremental authorization fails, a
        [card_declined](https://docs.stripe.com/docs/error-codes#card-declined) error returns, and no other
        fields on the PaymentIntent or Charge update. The PaymentIntent
        object remains capturable for the previously authorized amount.

        Each PaymentIntent can have a maximum of 10 incremental authorization attempts, including declines.
        After it's captured, a PaymentIntent can no longer be incremented.

        Learn more about [incremental authorizations](https://docs.stripe.com/docs/terminal/features/incremental-authorizations).
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/increment_authorization".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def verify_microdeposits(
        self,
        intent: str,
        params: Optional["PaymentIntentVerifyMicrodepositsParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        return cast(
            "PaymentIntent",
            self._request(
                "post",
                "/v1/payment_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def verify_microdeposits_async(
        self,
        intent: str,
        params: Optional["PaymentIntentVerifyMicrodepositsParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "PaymentIntent":
        """
        Verifies microdeposits on a PaymentIntent object.
        """
        return cast(
            "PaymentIntent",
            await self._request_async(
                "post",
                "/v1/payment_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
