# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from typing import ClassVar
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._tax_code_list_params import TaxCodeListParams
    from stripe.params._tax_code_retrieve_params import TaxCodeRetrieveParams


class TaxCode(ListableAPIResource["TaxCode"]):
    """
    [Tax codes](https://stripe.com/docs/tax/tax-categories) classify goods and services for tax purposes.
    """

    OBJECT_NAME: ClassVar[Literal["tax_code"]] = "tax_code"
    description: str
    """
    A detailed description of which types of products the tax code represents.
    """
    id: str
    """
    Unique identifier for the object.
    """
    name: str
    """
    A short name for the tax code.
    """
    object: Literal["tax_code"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def list(
        cls, **params: Unpack["TaxCodeListParams"]
    ) -> ListObject["TaxCode"]:
        """
        A list of [all tax codes available](https://stripe.com/docs/tax/tax-categories) to add to Products in order to allow specific tax calculations.
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
        cls, **params: Unpack["TaxCodeListParams"]
    ) -> ListObject["TaxCode"]:
        """
        A list of [all tax codes available](https://stripe.com/docs/tax/tax-categories) to add to Products in order to allow specific tax calculations.
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
        cls, id: str, **params: Unpack["TaxCodeRetrieveParams"]
    ) -> "TaxCode":
        """
        Retrieves the details of an existing tax code. Supply the unique tax code ID and Stripe will return the corresponding tax code information.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TaxCodeRetrieveParams"]
    ) -> "TaxCode":
        """
        Retrieves the details of an existing tax code. Supply the unique tax code ID and Stripe will return the corresponding tax code information.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
