# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._payment_method_domain_create_params import (
        PaymentMethodDomainCreateParams,
    )
    from stripe.params._payment_method_domain_list_params import (
        PaymentMethodDomainListParams,
    )
    from stripe.params._payment_method_domain_modify_params import (
        PaymentMethodDomainModifyParams,
    )
    from stripe.params._payment_method_domain_retrieve_params import (
        PaymentMethodDomainRetrieveParams,
    )
    from stripe.params._payment_method_domain_validate_params import (
        PaymentMethodDomainValidateParams,
    )


class PaymentMethodDomain(
    CreateableAPIResource["PaymentMethodDomain"],
    ListableAPIResource["PaymentMethodDomain"],
    UpdateableAPIResource["PaymentMethodDomain"],
):
    """
    A payment method domain represents a web domain that you have registered with Stripe.
    Stripe Elements use registered payment method domains to control where certain payment methods are shown.

    Related guide: [Payment method domains](https://docs.stripe.com/payments/payment-methods/pmd-registration).
    """

    OBJECT_NAME: ClassVar[Literal["payment_method_domain"]] = (
        "payment_method_domain"
    )

    class AmazonPay(StripeObject):
        class StatusDetails(StripeObject):
            error_message: str
            """
            The error message associated with the status of the payment method on the domain.
            """

        status: Literal["active", "inactive"]
        """
        The status of the payment method on the domain.
        """
        status_details: Optional[StatusDetails]
        """
        Contains additional details about the status of a payment method for a specific payment method domain.
        """
        _inner_class_types = {"status_details": StatusDetails}

    class ApplePay(StripeObject):
        class StatusDetails(StripeObject):
            error_message: str
            """
            The error message associated with the status of the payment method on the domain.
            """

        status: Literal["active", "inactive"]
        """
        The status of the payment method on the domain.
        """
        status_details: Optional[StatusDetails]
        """
        Contains additional details about the status of a payment method for a specific payment method domain.
        """
        _inner_class_types = {"status_details": StatusDetails}

    class GooglePay(StripeObject):
        class StatusDetails(StripeObject):
            error_message: str
            """
            The error message associated with the status of the payment method on the domain.
            """

        status: Literal["active", "inactive"]
        """
        The status of the payment method on the domain.
        """
        status_details: Optional[StatusDetails]
        """
        Contains additional details about the status of a payment method for a specific payment method domain.
        """
        _inner_class_types = {"status_details": StatusDetails}

    class Klarna(StripeObject):
        class StatusDetails(StripeObject):
            error_message: str
            """
            The error message associated with the status of the payment method on the domain.
            """

        status: Literal["active", "inactive"]
        """
        The status of the payment method on the domain.
        """
        status_details: Optional[StatusDetails]
        """
        Contains additional details about the status of a payment method for a specific payment method domain.
        """
        _inner_class_types = {"status_details": StatusDetails}

    class Link(StripeObject):
        class StatusDetails(StripeObject):
            error_message: str
            """
            The error message associated with the status of the payment method on the domain.
            """

        status: Literal["active", "inactive"]
        """
        The status of the payment method on the domain.
        """
        status_details: Optional[StatusDetails]
        """
        Contains additional details about the status of a payment method for a specific payment method domain.
        """
        _inner_class_types = {"status_details": StatusDetails}

    class Paypal(StripeObject):
        class StatusDetails(StripeObject):
            error_message: str
            """
            The error message associated with the status of the payment method on the domain.
            """

        status: Literal["active", "inactive"]
        """
        The status of the payment method on the domain.
        """
        status_details: Optional[StatusDetails]
        """
        Contains additional details about the status of a payment method for a specific payment method domain.
        """
        _inner_class_types = {"status_details": StatusDetails}

    amazon_pay: AmazonPay
    """
    Indicates the status of a specific payment method on a payment method domain.
    """
    apple_pay: ApplePay
    """
    Indicates the status of a specific payment method on a payment method domain.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    domain_name: str
    """
    The domain name that this payment method domain object represents.
    """
    enabled: bool
    """
    Whether this payment method domain is enabled. If the domain is not enabled, payment methods that require a payment method domain will not appear in Elements.
    """
    google_pay: GooglePay
    """
    Indicates the status of a specific payment method on a payment method domain.
    """
    id: str
    """
    Unique identifier for the object.
    """
    klarna: Klarna
    """
    Indicates the status of a specific payment method on a payment method domain.
    """
    link: Link
    """
    Indicates the status of a specific payment method on a payment method domain.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["payment_method_domain"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    paypal: Paypal
    """
    Indicates the status of a specific payment method on a payment method domain.
    """

    @classmethod
    def create(
        cls, **params: Unpack["PaymentMethodDomainCreateParams"]
    ) -> "PaymentMethodDomain":
        """
        Creates a payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PaymentMethodDomainCreateParams"]
    ) -> "PaymentMethodDomain":
        """
        Creates a payment method domain.
        """
        return cast(
            "PaymentMethodDomain",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PaymentMethodDomainListParams"]
    ) -> ListObject["PaymentMethodDomain"]:
        """
        Lists the details of existing payment method domains.
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
        cls, **params: Unpack["PaymentMethodDomainListParams"]
    ) -> ListObject["PaymentMethodDomain"]:
        """
        Lists the details of existing payment method domains.
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
    def modify(
        cls, id: str, **params: Unpack["PaymentMethodDomainModifyParams"]
    ) -> "PaymentMethodDomain":
        """
        Updates an existing payment method domain.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentMethodDomain",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["PaymentMethodDomainModifyParams"]
    ) -> "PaymentMethodDomain":
        """
        Updates an existing payment method domain.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentMethodDomain",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PaymentMethodDomainRetrieveParams"]
    ) -> "PaymentMethodDomain":
        """
        Retrieves the details of an existing payment method domain.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PaymentMethodDomainRetrieveParams"]
    ) -> "PaymentMethodDomain":
        """
        Retrieves the details of an existing payment method domain.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_validate(
        cls,
        payment_method_domain: str,
        **params: Unpack["PaymentMethodDomainValidateParams"],
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        return cast(
            "PaymentMethodDomain",
            cls._static_request(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}/validate".format(
                    payment_method_domain=sanitize_id(payment_method_domain)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def validate(
        payment_method_domain: str,
        **params: Unpack["PaymentMethodDomainValidateParams"],
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        ...

    @overload
    def validate(
        self, **params: Unpack["PaymentMethodDomainValidateParams"]
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        ...

    @class_method_variant("_cls_validate")
    def validate(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentMethodDomainValidateParams"]
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        return cast(
            "PaymentMethodDomain",
            self._request(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}/validate".format(
                    payment_method_domain=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_validate_async(
        cls,
        payment_method_domain: str,
        **params: Unpack["PaymentMethodDomainValidateParams"],
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        return cast(
            "PaymentMethodDomain",
            await cls._static_request_async(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}/validate".format(
                    payment_method_domain=sanitize_id(payment_method_domain)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def validate_async(
        payment_method_domain: str,
        **params: Unpack["PaymentMethodDomainValidateParams"],
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        ...

    @overload
    async def validate_async(
        self, **params: Unpack["PaymentMethodDomainValidateParams"]
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        ...

    @class_method_variant("_cls_validate_async")
    async def validate_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentMethodDomainValidateParams"]
    ) -> "PaymentMethodDomain":
        """
        Some payment methods might require additional steps to register a domain. If the requirements weren't satisfied when the domain was created, the payment method will be inactive on the domain.
        The payment method doesn't appear in Elements or Embedded Checkout for this domain until it is active.

        To activate a payment method on an existing payment method domain, complete the required registration steps specific to the payment method, and then validate the payment method domain with this endpoint.

        Related guides: [Payment method domains](https://docs.stripe.com/docs/payments/payment-methods/pmd-registration).
        """
        return cast(
            "PaymentMethodDomain",
            await self._request_async(
                "post",
                "/v1/payment_method_domains/{payment_method_domain}/validate".format(
                    payment_method_domain=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "amazon_pay": AmazonPay,
        "apple_pay": ApplePay,
        "google_pay": GooglePay,
        "klarna": Klarna,
        "link": Link,
        "paypal": Paypal,
    }
