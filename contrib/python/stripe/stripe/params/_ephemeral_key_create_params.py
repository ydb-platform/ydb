# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import NotRequired, TypedDict


class EphemeralKeyCreateParams(TypedDict):
    customer: NotRequired[str]
    """
    The ID of the Customer you'd like to modify using the resulting ephemeral key.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    issuing_card: NotRequired[str]
    """
    The ID of the Issuing Card you'd like to access using the resulting ephemeral key.
    """
    nonce: NotRequired[str]
    """
    A single-use token, created by Stripe.js, used for creating ephemeral keys for Issuing Cards without exchanging sensitive information.
    """
    verification_session: NotRequired[str]
    """
    The ID of the Identity VerificationSession you'd like to access using the resulting ephemeral key
    """
