# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._setup_intent import SetupIntent
    from stripe.params._setup_intent_cancel_params import (
        SetupIntentCancelParams,
    )
    from stripe.params._setup_intent_confirm_params import (
        SetupIntentConfirmParams,
    )
    from stripe.params._setup_intent_create_params import (
        SetupIntentCreateParams,
    )
    from stripe.params._setup_intent_list_params import SetupIntentListParams
    from stripe.params._setup_intent_retrieve_params import (
        SetupIntentRetrieveParams,
    )
    from stripe.params._setup_intent_update_params import (
        SetupIntentUpdateParams,
    )
    from stripe.params._setup_intent_verify_microdeposits_params import (
        SetupIntentVerifyMicrodepositsParams,
    )


class SetupIntentService(StripeService):
    def list(
        self,
        params: Optional["SetupIntentListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SetupIntent]":
        """
        Returns a list of SetupIntents.
        """
        return cast(
            "ListObject[SetupIntent]",
            self._request(
                "get",
                "/v1/setup_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["SetupIntentListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SetupIntent]":
        """
        Returns a list of SetupIntents.
        """
        return cast(
            "ListObject[SetupIntent]",
            await self._request_async(
                "get",
                "/v1/setup_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["SetupIntentCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Creates a SetupIntent object.

        After you create the SetupIntent, attach a payment method and [confirm](https://docs.stripe.com/docs/api/setup_intents/confirm)
        it to collect any required permissions to charge the payment method later.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["SetupIntentCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Creates a SetupIntent object.

        After you create the SetupIntent, attach a payment method and [confirm](https://docs.stripe.com/docs/api/setup_intents/confirm)
        it to collect any required permissions to charge the payment method later.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        intent: str,
        params: Optional["SetupIntentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Retrieves the details of a SetupIntent that has previously been created.

        Client-side retrieval using a publishable key is allowed when the client_secret is provided in the query string.

        When retrieved with a publishable key, only a subset of properties will be returned. Please refer to the [SetupIntent](https://docs.stripe.com/api#setup_intent_object) object reference for more details.
        """
        return cast(
            "SetupIntent",
            self._request(
                "get",
                "/v1/setup_intents/{intent}".format(
                    intent=sanitize_id(intent)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        intent: str,
        params: Optional["SetupIntentRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Retrieves the details of a SetupIntent that has previously been created.

        Client-side retrieval using a publishable key is allowed when the client_secret is provided in the query string.

        When retrieved with a publishable key, only a subset of properties will be returned. Please refer to the [SetupIntent](https://docs.stripe.com/api#setup_intent_object) object reference for more details.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "get",
                "/v1/setup_intents/{intent}".format(
                    intent=sanitize_id(intent)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        intent: str,
        params: Optional["SetupIntentUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Updates a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}".format(
                    intent=sanitize_id(intent)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        intent: str,
        params: Optional["SetupIntentUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Updates a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}".format(
                    intent=sanitize_id(intent)
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        intent: str,
        params: Optional["SetupIntentCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}/cancel".format(
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
        params: Optional["SetupIntentCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        You can cancel a SetupIntent object when it's in one of these statuses: requires_payment_method, requires_confirmation, or requires_action.

        After you cancel it, setup is abandoned and any operations on the SetupIntent fail with an error. You can't cancel the SetupIntent for a Checkout Session. [Expire the Checkout Session](https://docs.stripe.com/docs/api/checkout/sessions/expire) instead.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}/cancel".format(
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
        params: Optional["SetupIntentConfirmParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}/confirm".format(
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
        params: Optional["SetupIntentConfirmParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Confirm that your customer intends to set up the current or
        provided payment method. For example, you would confirm a SetupIntent
        when a customer hits the “Save” button on a payment method management
        page on your website.

        If the selected payment method does not require any additional
        steps from the customer, the SetupIntent will transition to the
        succeeded status.

        Otherwise, it will transition to the requires_action status and
        suggest additional actions via next_action. If setup fails,
        the SetupIntent will transition to the
        requires_payment_method status or the canceled status if the
        confirmation limit is reached.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}/confirm".format(
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
        params: Optional["SetupIntentVerifyMicrodepositsParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            self._request(
                "post",
                "/v1/setup_intents/{intent}/verify_microdeposits".format(
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
        params: Optional["SetupIntentVerifyMicrodepositsParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "SetupIntent":
        """
        Verifies microdeposits on a SetupIntent object.
        """
        return cast(
            "SetupIntent",
            await self._request_async(
                "post",
                "/v1/setup_intents/{intent}/verify_microdeposits".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
