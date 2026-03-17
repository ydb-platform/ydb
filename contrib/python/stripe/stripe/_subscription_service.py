# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._discount import Discount
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe._subscription import Subscription
    from stripe.params._subscription_cancel_params import (
        SubscriptionCancelParams,
    )
    from stripe.params._subscription_create_params import (
        SubscriptionCreateParams,
    )
    from stripe.params._subscription_delete_discount_params import (
        SubscriptionDeleteDiscountParams,
    )
    from stripe.params._subscription_list_params import SubscriptionListParams
    from stripe.params._subscription_migrate_params import (
        SubscriptionMigrateParams,
    )
    from stripe.params._subscription_resume_params import (
        SubscriptionResumeParams,
    )
    from stripe.params._subscription_retrieve_params import (
        SubscriptionRetrieveParams,
    )
    from stripe.params._subscription_search_params import (
        SubscriptionSearchParams,
    )
    from stripe.params._subscription_update_params import (
        SubscriptionUpdateParams,
    )


class SubscriptionService(StripeService):
    def cancel(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        return cast(
            "Subscription",
            self._request(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Cancels a customer's subscription immediately. The customer won't be charged again for the subscription. After it's canceled, you can no longer update the subscription or its [metadata](https://docs.stripe.com/metadata).

        Any pending invoice items that you've created are still charged at the end of the period, unless manually [deleted](https://docs.stripe.com/api#delete_invoiceitem). If you've set the subscription to cancel at the end of the period, any pending prorations are also left in place and collected at the end of the period. But if the subscription is set to cancel immediately, pending prorations are removed if invoice_now and prorate are both set to true.

        By default, upon subscription cancellation, Stripe stops automatic collection of all finalized invoices for the customer. This is intended to prevent unexpected payment attempts after the customer has canceled a subscription. However, you can resume automatic collection of the invoices manually after subscription cancellation to have us proceed. Or, you could check for unpaid invoices before allowing the customer to cancel the subscription at all.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Retrieves the subscription with the given ID.
        """
        return cast(
            "Subscription",
            self._request(
                "get",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Retrieves the subscription with the given ID.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "get",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Updates an existing subscription to match the specified parameters.
        When changing prices or quantities, we optionally prorate the price we charge next month to make up for any price changes.
        To preview how the proration is calculated, use the [create preview](https://docs.stripe.com/docs/api/invoices/create_preview) endpoint.

        By default, we prorate subscription changes. For example, if a customer signs up on May 1 for a 100 price, they'll be billed 100 immediately. If on May 15 they switch to a 200 price, then on June 1 they'll be billed 250 (200 for a renewal of her subscription, plus a 50 prorating adjustment for half of the previous month's 100 difference). Similarly, a downgrade generates a credit that is applied to the next invoice. We also prorate when you make quantity changes.

        Switching prices does not normally change the billing date or generate an immediate charge unless:


        The billing interval is changed (for example, from monthly to yearly).
        The subscription moves from free to paid.
        A trial starts or ends.


        In these cases, we apply a credit for the unused time on the previous price, immediately charge the customer using the new price, and reset the billing date. Learn about how [Stripe immediately attempts payment for subscription changes](https://docs.stripe.com/docs/billing/subscriptions/upgrade-downgrade#immediate-payment).

        If you want to charge for an upgrade immediately, pass proration_behavior as always_invoice to create prorations, automatically invoice the customer for those proration adjustments, and attempt to collect payment. If you pass create_prorations, the prorations are created but not automatically invoiced. If you want to bill the customer for the prorations before the subscription's renewal date, you need to manually [invoice the customer](https://docs.stripe.com/docs/api/invoices/create).

        If you don't want to prorate, set the proration_behavior option to none. With this option, the customer is billed 100 on May 1 and 200 on June 1. Similarly, if you set proration_behavior to none when switching between different billing intervals (for example, from monthly to yearly), we don't generate any credits for the old subscription's unused time. We still reset the billing date and bill immediately for the new subscription.

        Updating the quantity on a subscription many times in an hour may result in [rate limiting. If you need to bill for a frequently changing quantity, consider integrating <a href="/docs/billing/subscriptions/usage-based">usage-based billing](https://docs.stripe.com/docs/rate-limits) instead.
        """
        return cast(
            "Subscription",
            self._request(
                "post",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Updates an existing subscription to match the specified parameters.
        When changing prices or quantities, we optionally prorate the price we charge next month to make up for any price changes.
        To preview how the proration is calculated, use the [create preview](https://docs.stripe.com/docs/api/invoices/create_preview) endpoint.

        By default, we prorate subscription changes. For example, if a customer signs up on May 1 for a 100 price, they'll be billed 100 immediately. If on May 15 they switch to a 200 price, then on June 1 they'll be billed 250 (200 for a renewal of her subscription, plus a 50 prorating adjustment for half of the previous month's 100 difference). Similarly, a downgrade generates a credit that is applied to the next invoice. We also prorate when you make quantity changes.

        Switching prices does not normally change the billing date or generate an immediate charge unless:


        The billing interval is changed (for example, from monthly to yearly).
        The subscription moves from free to paid.
        A trial starts or ends.


        In these cases, we apply a credit for the unused time on the previous price, immediately charge the customer using the new price, and reset the billing date. Learn about how [Stripe immediately attempts payment for subscription changes](https://docs.stripe.com/docs/billing/subscriptions/upgrade-downgrade#immediate-payment).

        If you want to charge for an upgrade immediately, pass proration_behavior as always_invoice to create prorations, automatically invoice the customer for those proration adjustments, and attempt to collect payment. If you pass create_prorations, the prorations are created but not automatically invoiced. If you want to bill the customer for the prorations before the subscription's renewal date, you need to manually [invoice the customer](https://docs.stripe.com/docs/api/invoices/create).

        If you don't want to prorate, set the proration_behavior option to none. With this option, the customer is billed 100 on May 1 and 200 on June 1. Similarly, if you set proration_behavior to none when switching between different billing intervals (for example, from monthly to yearly), we don't generate any credits for the old subscription's unused time. We still reset the billing date and bill immediately for the new subscription.

        Updating the quantity on a subscription many times in an hour may result in [rate limiting. If you need to bill for a frequently changing quantity, consider integrating <a href="/docs/billing/subscriptions/usage-based">usage-based billing](https://docs.stripe.com/docs/rate-limits) instead.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "post",
                "/v1/subscriptions/{subscription_exposed_id}".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def delete_discount(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionDeleteDiscountParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        return cast(
            "Discount",
            self._request(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}/discount".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_discount_async(
        self,
        subscription_exposed_id: str,
        params: Optional["SubscriptionDeleteDiscountParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Discount":
        """
        Removes the currently applied discount on a subscription.
        """
        return cast(
            "Discount",
            await self._request_async(
                "delete",
                "/v1/subscriptions/{subscription_exposed_id}/discount".format(
                    subscription_exposed_id=sanitize_id(
                        subscription_exposed_id
                    ),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["SubscriptionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Subscription]":
        """
        By default, returns a list of subscriptions that have not been canceled. In order to list canceled subscriptions, specify status=canceled.
        """
        return cast(
            "ListObject[Subscription]",
            self._request(
                "get",
                "/v1/subscriptions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["SubscriptionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Subscription]":
        """
        By default, returns a list of subscriptions that have not been canceled. In order to list canceled subscriptions, specify status=canceled.
        """
        return cast(
            "ListObject[Subscription]",
            await self._request_async(
                "get",
                "/v1/subscriptions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["SubscriptionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Creates a new subscription on an existing customer. Each customer can have up to 500 active or scheduled subscriptions.

        When you create a subscription with collection_method=charge_automatically, the first invoice is finalized as part of the request.
        The payment_behavior parameter determines the exact behavior of the initial payment.

        To start subscriptions where the first invoice always begins in a draft status, use [subscription schedules](https://docs.stripe.com/docs/billing/subscriptions/subscription-schedules#managing) instead.
        Schedules provide the flexibility to model more complex billing configurations that change over time.
        """
        return cast(
            "Subscription",
            self._request(
                "post",
                "/v1/subscriptions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["SubscriptionCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Creates a new subscription on an existing customer. Each customer can have up to 500 active or scheduled subscriptions.

        When you create a subscription with collection_method=charge_automatically, the first invoice is finalized as part of the request.
        The payment_behavior parameter determines the exact behavior of the initial payment.

        To start subscriptions where the first invoice always begins in a draft status, use [subscription schedules](https://docs.stripe.com/docs/billing/subscriptions/subscription-schedules#managing) instead.
        Schedules provide the flexibility to model more complex billing configurations that change over time.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "post",
                "/v1/subscriptions",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "SubscriptionSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Subscription]":
        """
        Search for subscriptions you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Subscription]",
            self._request(
                "get",
                "/v1/subscriptions/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "SubscriptionSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Subscription]":
        """
        Search for subscriptions you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Subscription]",
            await self._request_async(
                "get",
                "/v1/subscriptions/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def migrate(
        self,
        subscription: str,
        params: "SubscriptionMigrateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        return cast(
            "Subscription",
            self._request(
                "post",
                "/v1/subscriptions/{subscription}/migrate".format(
                    subscription=sanitize_id(subscription),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def migrate_async(
        self,
        subscription: str,
        params: "SubscriptionMigrateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Upgrade the billing_mode of an existing subscription.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "post",
                "/v1/subscriptions/{subscription}/migrate".format(
                    subscription=sanitize_id(subscription),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def resume(
        self,
        subscription: str,
        params: Optional["SubscriptionResumeParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        return cast(
            "Subscription",
            self._request(
                "post",
                "/v1/subscriptions/{subscription}/resume".format(
                    subscription=sanitize_id(subscription),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def resume_async(
        self,
        subscription: str,
        params: Optional["SubscriptionResumeParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Subscription":
        """
        Initiates resumption of a paused subscription, optionally resetting the billing cycle anchor and creating prorations. If no resumption invoice is generated, the subscription becomes active immediately. If a resumption invoice is generated, the subscription remains paused until the invoice is paid or marked uncollectible. If the invoice is not paid by the expiration date, it is voided and the subscription remains paused.
        """
        return cast(
            "Subscription",
            await self._request_async(
                "post",
                "/v1/subscriptions/{subscription}/resume".format(
                    subscription=sanitize_id(subscription),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
