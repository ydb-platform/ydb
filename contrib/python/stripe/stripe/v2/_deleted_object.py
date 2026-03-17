# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import Optional


class DeletedObject(StripeObject):
    id: str
    """
    The ID of the object that's being deleted.
    """
    object: Optional[str]
    """
    String representing the type of the object that has been deleted. Objects of the same type share the same value of the object field.
    """
