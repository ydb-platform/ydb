# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal


class AccountOwner(StripeObject):
    """
    Describes an owner of an account.
    """

    OBJECT_NAME: ClassVar[Literal["financial_connections.account_owner"]] = (
        "financial_connections.account_owner"
    )
    email: Optional[str]
    """
    The email address of the owner.
    """
    id: str
    """
    Unique identifier for the object.
    """
    name: str
    """
    The full name of the owner.
    """
    object: Literal["financial_connections.account_owner"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    ownership: str
    """
    The ownership object that this owner belongs to.
    """
    phone: Optional[str]
    """
    The raw phone number of the owner.
    """
    raw_address: Optional[str]
    """
    The raw physical address of the owner.
    """
    refreshed_at: Optional[int]
    """
    The timestamp of the refresh that updated this owner.
    """
