# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._customer import Customer
    from stripe.billing._meter import Meter
    from stripe.params.billing._alert_activate_params import (
        AlertActivateParams,
    )
    from stripe.params.billing._alert_archive_params import AlertArchiveParams
    from stripe.params.billing._alert_create_params import AlertCreateParams
    from stripe.params.billing._alert_deactivate_params import (
        AlertDeactivateParams,
    )
    from stripe.params.billing._alert_list_params import AlertListParams
    from stripe.params.billing._alert_retrieve_params import (
        AlertRetrieveParams,
    )


class Alert(CreateableAPIResource["Alert"], ListableAPIResource["Alert"]):
    """
    A billing alert is a resource that notifies you when a certain usage threshold on a meter is crossed. For example, you might create a billing alert to notify you when a certain user made 100 API requests.
    """

    OBJECT_NAME: ClassVar[Literal["billing.alert"]] = "billing.alert"

    class UsageThreshold(StripeObject):
        class Filter(StripeObject):
            customer: Optional[ExpandableField["Customer"]]
            """
            Limit the scope of the alert to this customer ID
            """
            type: Literal["customer"]

        filters: Optional[List[Filter]]
        """
        The filters allow limiting the scope of this usage alert. You can only specify up to one filter at this time.
        """
        gte: int
        """
        The value at which this alert will trigger.
        """
        meter: ExpandableField["Meter"]
        """
        The [Billing Meter](https://docs.stripe.com/api/billing/meter) ID whose usage is monitored.
        """
        recurrence: Literal["one_time"]
        """
        Defines how the alert will behave.
        """
        _inner_class_types = {"filters": Filter}

    alert_type: Literal["usage_threshold"]
    """
    Defines the type of the alert.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["billing.alert"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Optional[Literal["active", "archived", "inactive"]]
    """
    Status of the alert. This can be active, inactive or archived.
    """
    title: str
    """
    Title of the alert.
    """
    usage_threshold: Optional[UsageThreshold]
    """
    Encapsulates configuration of the alert to monitor usage on a specific [Billing Meter](https://docs.stripe.com/api/billing/meter).
    """

    @classmethod
    def _cls_activate(
        cls, id: str, **params: Unpack["AlertActivateParams"]
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        return cast(
            "Alert",
            cls._static_request(
                "post",
                "/v1/billing/alerts/{id}/activate".format(id=sanitize_id(id)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def activate(id: str, **params: Unpack["AlertActivateParams"]) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        ...

    @overload
    def activate(self, **params: Unpack["AlertActivateParams"]) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        ...

    @class_method_variant("_cls_activate")
    def activate(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AlertActivateParams"]
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts/{id}/activate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_activate_async(
        cls, id: str, **params: Unpack["AlertActivateParams"]
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        return cast(
            "Alert",
            await cls._static_request_async(
                "post",
                "/v1/billing/alerts/{id}/activate".format(id=sanitize_id(id)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def activate_async(
        id: str, **params: Unpack["AlertActivateParams"]
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        ...

    @overload
    async def activate_async(
        self, **params: Unpack["AlertActivateParams"]
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        ...

    @class_method_variant("_cls_activate_async")
    async def activate_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AlertActivateParams"]
    ) -> "Alert":
        """
        Reactivates this alert, allowing it to trigger again.
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts/{id}/activate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_archive(
        cls, id: str, **params: Unpack["AlertArchiveParams"]
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        return cast(
            "Alert",
            cls._static_request(
                "post",
                "/v1/billing/alerts/{id}/archive".format(id=sanitize_id(id)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def archive(id: str, **params: Unpack["AlertArchiveParams"]) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        ...

    @overload
    def archive(self, **params: Unpack["AlertArchiveParams"]) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        ...

    @class_method_variant("_cls_archive")
    def archive(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AlertArchiveParams"]
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts/{id}/archive".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_archive_async(
        cls, id: str, **params: Unpack["AlertArchiveParams"]
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        return cast(
            "Alert",
            await cls._static_request_async(
                "post",
                "/v1/billing/alerts/{id}/archive".format(id=sanitize_id(id)),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def archive_async(
        id: str, **params: Unpack["AlertArchiveParams"]
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        ...

    @overload
    async def archive_async(
        self, **params: Unpack["AlertArchiveParams"]
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        ...

    @class_method_variant("_cls_archive_async")
    async def archive_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AlertArchiveParams"]
    ) -> "Alert":
        """
        Archives this alert, removing it from the list view and APIs. This is non-reversible.
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts/{id}/archive".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["AlertCreateParams"]) -> "Alert":
        """
        Creates a billing alert
        """
        return cast(
            "Alert",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["AlertCreateParams"]
    ) -> "Alert":
        """
        Creates a billing alert
        """
        return cast(
            "Alert",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_deactivate(
        cls, id: str, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        return cast(
            "Alert",
            cls._static_request(
                "post",
                "/v1/billing/alerts/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def deactivate(
        id: str, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        ...

    @overload
    def deactivate(self, **params: Unpack["AlertDeactivateParams"]) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        ...

    @class_method_variant("_cls_deactivate")
    def deactivate(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        return cast(
            "Alert",
            self._request(
                "post",
                "/v1/billing/alerts/{id}/deactivate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_deactivate_async(
        cls, id: str, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        return cast(
            "Alert",
            await cls._static_request_async(
                "post",
                "/v1/billing/alerts/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def deactivate_async(
        id: str, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        ...

    @overload
    async def deactivate_async(
        self, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        ...

    @class_method_variant("_cls_deactivate_async")
    async def deactivate_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AlertDeactivateParams"]
    ) -> "Alert":
        """
        Deactivates this alert, preventing it from triggering.
        """
        return cast(
            "Alert",
            await self._request_async(
                "post",
                "/v1/billing/alerts/{id}/deactivate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["AlertListParams"]) -> ListObject["Alert"]:
        """
        Lists billing active and inactive alerts
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
        cls, **params: Unpack["AlertListParams"]
    ) -> ListObject["Alert"]:
        """
        Lists billing active and inactive alerts
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
        cls, id: str, **params: Unpack["AlertRetrieveParams"]
    ) -> "Alert":
        """
        Retrieves a billing alert given an ID
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["AlertRetrieveParams"]
    ) -> "Alert":
        """
        Retrieves a billing alert given an ID
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"usage_threshold": UsageThreshold}
