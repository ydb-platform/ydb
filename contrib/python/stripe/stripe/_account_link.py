# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from typing import ClassVar, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._account_link_create_params import (
        AccountLinkCreateParams,
    )


class AccountLink(CreateableAPIResource["AccountLink"]):
    """
    Account Links are the means by which a Connect platform grants a connected account permission to access
    Stripe-hosted applications, such as Connect Onboarding.

    Related guide: [Connect Onboarding](https://docs.stripe.com/connect/custom/hosted-onboarding)
    """

    OBJECT_NAME: ClassVar[Literal["account_link"]] = "account_link"
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    expires_at: int
    """
    The timestamp at which this account link will expire.
    """
    object: Literal["account_link"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    url: str
    """
    The URL for the account link.
    """

    @classmethod
    def create(
        cls, **params: Unpack["AccountLinkCreateParams"]
    ) -> "AccountLink":
        """
        Creates an AccountLink object that includes a single-use Stripe URL that the platform can redirect their user to in order to take them through the Connect Onboarding flow.
        """
        return cast(
            "AccountLink",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["AccountLinkCreateParams"]
    ) -> "AccountLink":
        """
        Creates an AccountLink object that includes a single-use Stripe URL that the platform can redirect their user to in order to take them through the Connect Onboarding flow.
        """
        return cast(
            "AccountLink",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )
