# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from typing import ClassVar, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._charge import Charge
    from stripe._payment_intent import PaymentIntent
    from stripe.params.radar._early_fraud_warning_list_params import (
        EarlyFraudWarningListParams,
    )
    from stripe.params.radar._early_fraud_warning_retrieve_params import (
        EarlyFraudWarningRetrieveParams,
    )


class EarlyFraudWarning(ListableAPIResource["EarlyFraudWarning"]):
    """
    An early fraud warning indicates that the card issuer has notified us that a
    charge may be fraudulent.

    Related guide: [Early fraud warnings](https://docs.stripe.com/disputes/measuring#early-fraud-warnings)
    """

    OBJECT_NAME: ClassVar[Literal["radar.early_fraud_warning"]] = (
        "radar.early_fraud_warning"
    )
    actionable: bool
    """
    An EFW is actionable if it has not received a dispute and has not been fully refunded. You may wish to proactively refund a charge that receives an EFW, in order to avoid receiving a dispute later.
    """
    charge: ExpandableField["Charge"]
    """
    ID of the charge this early fraud warning is for, optionally expanded.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    fraud_type: str
    """
    The type of fraud labelled by the issuer. One of `card_never_received`, `fraudulent_card_application`, `made_with_counterfeit_card`, `made_with_lost_card`, `made_with_stolen_card`, `misc`, `unauthorized_use_of_card`.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["radar.early_fraud_warning"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payment_intent: Optional[ExpandableField["PaymentIntent"]]
    """
    ID of the Payment Intent this early fraud warning is for, optionally expanded.
    """

    @classmethod
    def list(
        cls, **params: Unpack["EarlyFraudWarningListParams"]
    ) -> ListObject["EarlyFraudWarning"]:
        """
        Returns a list of early fraud warnings.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["EarlyFraudWarningListParams"]
    ) -> ListObject["EarlyFraudWarning"]:
        """
        Returns a list of early fraud warnings.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["EarlyFraudWarningRetrieveParams"]
    ) -> "EarlyFraudWarning":
        """
        Retrieves the details of an early fraud warning that has previously been created.

        Please refer to the [early fraud warning](https://docs.stripe.com/api#early_fraud_warning_object) object reference for more details.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["EarlyFraudWarningRetrieveParams"]
    ) -> "EarlyFraudWarning":
        """
        Retrieves the details of an early fraud warning that has previously been created.

        Please refer to the [early fraud warning](https://docs.stripe.com/api#early_fraud_warning_object) object reference for more details.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
