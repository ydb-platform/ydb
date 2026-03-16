# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, List, Optional
from typing_extensions import Literal


class AccountLink(StripeObject):
    """
    Account Links let a platform create a temporary, single-use URL that an account can use to access a Stripe-hosted flow for collecting or updating required information.
    """

    OBJECT_NAME: ClassVar[Literal["v2.core.account_link"]] = (
        "v2.core.account_link"
    )

    class UseCase(StripeObject):
        class AccountOnboarding(StripeObject):
            class CollectionOptions(StripeObject):
                fields: Optional[Literal["currently_due", "eventually_due"]]
                """
                Specifies whether the platform collects only currently_due requirements (`currently_due`) or both currently_due and eventually_due requirements (`eventually_due`). If you don't specify collection_options, the default value is currently_due.
                """
                future_requirements: Optional[Literal["include", "omit"]]
                """
                Specifies whether the platform collects future_requirements in addition to requirements in Connect Onboarding. The default value is `omit`.
                """

            collection_options: Optional[CollectionOptions]
            """
            Specifies the requirements that Stripe collects from v2/core/accounts in the Onboarding flow.
            """
            configurations: List[Literal["customer", "merchant", "recipient"]]
            """
            Open Enum. A v2/core/account can be configured to enable certain functionality. The configuration param targets the v2/core/account_link to collect information for the specified v2/core/account configuration/s.
            """
            refresh_url: str
            """
            The URL the user will be redirected to if the AccountLink is expired, has been used, or is otherwise invalid. The URL you specify should attempt to generate a new AccountLink with the same parameters used to create the original AccountLink, then redirect the user to the new AccountLink's URL so they can continue the flow. If a new AccountLink cannot be generated or the redirect fails you should display a useful error to the user. Please make sure to implement authentication before redirecting the user in case this URL is leaked to a third party.
            """
            return_url: Optional[str]
            """
            The URL that the user will be redirected to upon completing the linked flow.
            """
            _inner_class_types = {"collection_options": CollectionOptions}

        class AccountUpdate(StripeObject):
            class CollectionOptions(StripeObject):
                fields: Optional[Literal["currently_due", "eventually_due"]]
                """
                Specifies whether the platform collects only currently_due requirements (`currently_due`) or both currently_due and eventually_due requirements (`eventually_due`). The default value is `currently_due`.
                """
                future_requirements: Optional[Literal["include", "omit"]]
                """
                Specifies whether the platform collects future_requirements in addition to requirements in Connect Onboarding. The default value is `omit`.
                """

            collection_options: Optional[CollectionOptions]
            """
            Specifies the requirements that Stripe collects from v2/core/accounts in the Onboarding flow.
            """
            configurations: List[Literal["customer", "merchant", "recipient"]]
            """
            Open Enum. A v2/account can be configured to enable certain functionality. The configuration param targets the v2/account_link to collect information for the specified v2/account configuration/s.
            """
            refresh_url: str
            """
            The URL the user will be redirected to if the Account Link is expired, has been used, or is otherwise invalid. The URL you specify should attempt to generate a new Account Link with the same parameters used to create the original Account Link, then redirect the user to the new Account Link URL so they can continue the flow. Make sure to authenticate the user before redirecting to the new Account Link, in case the URL leaks to a third party. If a new Account Link can't be generated, or if the redirect fails, you should display a useful error to the user.
            """
            return_url: Optional[str]
            """
            The URL that the user will be redirected to upon completing the linked flow.
            """
            _inner_class_types = {"collection_options": CollectionOptions}

        account_onboarding: Optional[AccountOnboarding]
        """
        Hash containing configuration options for an Account Link object that onboards a new account.
        """
        account_update: Optional[AccountUpdate]
        """
        Hash containing configuration options for an Account Link that updates an existing account.
        """
        type: Literal["account_onboarding", "account_update"]
        """
        Open Enum. The type of Account Link the user is requesting.
        """
        _inner_class_types = {
            "account_onboarding": AccountOnboarding,
            "account_update": AccountUpdate,
        }

    account: str
    """
    The ID of the connected account this Account Link applies to.
    """
    created: str
    """
    The timestamp at which this Account Link was created.
    """
    expires_at: str
    """
    The timestamp at which this Account Link will expire.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["v2.core.account_link"]
    """
    String representing the object's type. Objects of the same type share the same value of the object field.
    """
    url: str
    """
    The URL at which the account can access the Stripe-hosted flow.
    """
    use_case: UseCase
    """
    Hash containing usage options.
    """
    _inner_class_types = {"use_case": UseCase}
