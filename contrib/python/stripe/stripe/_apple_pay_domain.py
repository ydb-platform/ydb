# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._apple_pay_domain_create_params import (
        ApplePayDomainCreateParams,
    )
    from stripe.params._apple_pay_domain_delete_params import (
        ApplePayDomainDeleteParams,
    )
    from stripe.params._apple_pay_domain_list_params import (
        ApplePayDomainListParams,
    )
    from stripe.params._apple_pay_domain_retrieve_params import (
        ApplePayDomainRetrieveParams,
    )


class ApplePayDomain(
    CreateableAPIResource["ApplePayDomain"],
    DeletableAPIResource["ApplePayDomain"],
    ListableAPIResource["ApplePayDomain"],
):
    OBJECT_NAME: ClassVar[Literal["apple_pay_domain"]] = "apple_pay_domain"
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    domain_name: str
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["apple_pay_domain"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def create(
        cls, **params: Unpack["ApplePayDomainCreateParams"]
    ) -> "ApplePayDomain":
        """
        Create an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ApplePayDomainCreateParams"]
    ) -> "ApplePayDomain":
        """
        Create an apple pay domain.
        """
        return cast(
            "ApplePayDomain",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "ApplePayDomain",
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            "ApplePayDomain",
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ApplePayDomainDeleteParams"]
    ) -> "ApplePayDomain":
        """
        Delete an apple pay domain.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    def list(
        cls, **params: Unpack["ApplePayDomainListParams"]
    ) -> ListObject["ApplePayDomain"]:
        """
        List apple pay domains.
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
        cls, **params: Unpack["ApplePayDomainListParams"]
    ) -> ListObject["ApplePayDomain"]:
        """
        List apple pay domains.
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
        cls, id: str, **params: Unpack["ApplePayDomainRetrieveParams"]
    ) -> "ApplePayDomain":
        """
        Retrieve an apple pay domain.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ApplePayDomainRetrieveParams"]
    ) -> "ApplePayDomain":
        """
        Retrieve an apple pay domain.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def class_url(cls):
        return "/v1/apple_pay/domains"
