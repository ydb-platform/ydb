# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.financial_connections._account_owner import AccountOwner


class AccountOwnership(StripeObject):
    """
    Describes a snapshot of the owners of an account at a particular point in time.
    """

    OBJECT_NAME: ClassVar[
        Literal["financial_connections.account_ownership"]
    ] = "financial_connections.account_ownership"
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    object: Literal["financial_connections.account_ownership"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    owners: ListObject["AccountOwner"]
    """
    A paginated list of owners for this account.
    """
