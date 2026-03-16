# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._country_spec_list_params import CountrySpecListParams
    from stripe.params._country_spec_retrieve_params import (
        CountrySpecRetrieveParams,
    )


class CountrySpec(ListableAPIResource["CountrySpec"]):
    """
    Stripe needs to collect certain pieces of information about each account
    created. These requirements can differ depending on the account's country. The
    Country Specs API makes these rules available to your integration.

    You can also view the information from this API call as [an online
    guide](https://docs.stripe.com/docs/connect/required-verification-information).
    """

    OBJECT_NAME: ClassVar[Literal["country_spec"]] = "country_spec"

    class VerificationFields(StripeObject):
        class Company(StripeObject):
            additional: List[str]
            """
            Additional fields which are only required for some users.
            """
            minimum: List[str]
            """
            Fields which every account must eventually provide.
            """

        class Individual(StripeObject):
            additional: List[str]
            """
            Additional fields which are only required for some users.
            """
            minimum: List[str]
            """
            Fields which every account must eventually provide.
            """

        company: Company
        individual: Individual
        _inner_class_types = {"company": Company, "individual": Individual}

    default_currency: str
    """
    The default currency for this country. This applies to both payment methods and bank accounts.
    """
    id: str
    """
    Unique identifier for the object. Represented as the ISO country code for this country.
    """
    object: Literal["country_spec"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    supported_bank_account_currencies: Dict[str, List[str]]
    """
    Currencies that can be accepted in the specific country (for transfers).
    """
    supported_payment_currencies: List[str]
    """
    Currencies that can be accepted in the specified country (for payments).
    """
    supported_payment_methods: List[str]
    """
    Payment methods available in the specified country. You may need to enable some payment methods (e.g., [ACH](https://stripe.com/docs/ach)) on your account before they appear in this list. The `stripe` payment method refers to [charging through your platform](https://stripe.com/docs/connect/destination-charges).
    """
    supported_transfer_countries: List[str]
    """
    Countries that can accept transfers from the specified country.
    """
    verification_fields: VerificationFields

    @classmethod
    def list(
        cls, **params: Unpack["CountrySpecListParams"]
    ) -> ListObject["CountrySpec"]:
        """
        Lists all Country Spec objects available in the API.
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
        cls, **params: Unpack["CountrySpecListParams"]
    ) -> ListObject["CountrySpec"]:
        """
        Lists all Country Spec objects available in the API.
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
        cls, id: str, **params: Unpack["CountrySpecRetrieveParams"]
    ) -> "CountrySpec":
        """
        Returns a Country Spec for a given Country code.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["CountrySpecRetrieveParams"]
    ) -> "CountrySpec":
        """
        Returns a Country Spec for a given Country code.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"verification_fields": VerificationFields}
