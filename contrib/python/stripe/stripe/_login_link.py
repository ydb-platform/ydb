# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar
from typing_extensions import Literal


class LoginLink(StripeObject):
    """
    Login Links are single-use URLs that takes an Express account to the login page for their Stripe dashboard.
    A Login Link differs from an [Account Link](https://docs.stripe.com/api/account_links) in that it takes the user directly to their [Express dashboard for the specified account](https://docs.stripe.com/connect/integrate-express-dashboard#create-login-link)
    """

    OBJECT_NAME: ClassVar[Literal["login_link"]] = "login_link"
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    object: Literal["login_link"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    url: str
    """
    The URL for the login link.
    """
