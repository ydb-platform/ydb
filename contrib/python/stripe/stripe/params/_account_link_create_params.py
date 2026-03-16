# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class AccountLinkCreateParams(RequestOptions):
    account: str
    """
    The identifier of the account to create an account link for.
    """
    collect: NotRequired[Literal["currently_due", "eventually_due"]]
    """
    The collect parameter is deprecated. Use `collection_options` instead.
    """
    collection_options: NotRequired["AccountLinkCreateParamsCollectionOptions"]
    """
    Specifies the requirements that Stripe collects from connected accounts in the Connect Onboarding flow.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    refresh_url: NotRequired[str]
    """
    The URL the user will be redirected to if the account link is expired, has been previously-visited, or is otherwise invalid. The URL you specify should attempt to generate a new account link with the same parameters used to create the original account link, then redirect the user to the new account link's URL so they can continue with Connect Onboarding. If a new account link cannot be generated or the redirect fails you should display a useful error to the user.
    """
    return_url: NotRequired[str]
    """
    The URL that the user will be redirected to upon leaving or completing the linked flow.
    """
    type: Literal["account_onboarding", "account_update"]
    """
    The type of account link the user is requesting.

    You can create Account Links of type `account_update` only for connected accounts where your platform is responsible for collecting requirements, including Custom accounts. You can't create them for accounts that have access to a Stripe-hosted Dashboard. If you use [Connect embedded components](https://docs.stripe.com/connect/get-started-connect-embedded-components), you can include components that allow your connected accounts to update their own information. For an account without Stripe-hosted Dashboard access where Stripe is liable for negative balances, you must use embedded components.
    """


class AccountLinkCreateParamsCollectionOptions(TypedDict):
    fields: NotRequired[Literal["currently_due", "eventually_due"]]
    """
    Specifies whether the platform collects only currently_due requirements (`currently_due`) or both currently_due and eventually_due requirements (`eventually_due`). If you don't specify `collection_options`, the default value is `currently_due`.
    """
    future_requirements: NotRequired[Literal["include", "omit"]]
    """
    Specifies whether the platform collects future_requirements in addition to requirements in Connect Onboarding. The default value is `omit`.
    """
