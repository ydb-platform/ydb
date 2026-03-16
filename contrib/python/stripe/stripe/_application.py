# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal


class Application(StripeObject):
    OBJECT_NAME: ClassVar[Literal["application"]] = "application"
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    id: str
    """
    Unique identifier for the object.
    """
    name: Optional[str]
    """
    The name of the application.
    """
    object: Literal["application"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
