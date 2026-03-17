# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class OnboardingLinkCreateParams(RequestOptions):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    link_options: "OnboardingLinkCreateParamsLinkOptions"
    """
    Specific fields needed to generate the desired link type.
    """
    link_type: Literal["apple_terms_and_conditions"]
    """
    The type of link being generated.
    """
    on_behalf_of: NotRequired[str]
    """
    Stripe account ID to generate the link for.
    """


class OnboardingLinkCreateParamsLinkOptions(TypedDict):
    apple_terms_and_conditions: NotRequired[
        "OnboardingLinkCreateParamsLinkOptionsAppleTermsAndConditions"
    ]
    """
    The options associated with the Apple Terms and Conditions link type.
    """


class OnboardingLinkCreateParamsLinkOptionsAppleTermsAndConditions(TypedDict):
    allow_relinking: NotRequired[bool]
    """
    Whether the link should also support users relinking their Apple account.
    """
    merchant_display_name: str
    """
    The business name of the merchant accepting Apple's Terms and Conditions.
    """
