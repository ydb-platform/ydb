# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal


class AccountPersonToken(StripeObject):
    """
    Person Tokens are single-use tokens which tokenize person information, and are used for creating or updating a Person.
    """

    OBJECT_NAME: ClassVar[Literal["v2.core.account_person_token"]] = (
        "v2.core.account_person_token"
    )
    created: str
    """
    Time at which the token was created. Represented as a RFC 3339 date & time UTC value in millisecond precision, for example: 2022-09-18T13:22:18.123Z.
    """
    expires_at: str
    """
    Time at which the token will expire.
    """
    id: str
    """
    Unique identifier for the token.
    """
    livemode: bool
    """
    Has the value `true` if the token exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["v2.core.account_person_token"]
    """
    String representing the object's type. Objects of the same type share the same value of the object field.
    """
    used: bool
    """
    Determines if the token has already been used (tokens can only be used once).
    """
