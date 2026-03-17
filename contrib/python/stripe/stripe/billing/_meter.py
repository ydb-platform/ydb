# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.billing._meter_event_summary import MeterEventSummary
    from stripe.params.billing._meter_create_params import MeterCreateParams
    from stripe.params.billing._meter_deactivate_params import (
        MeterDeactivateParams,
    )
    from stripe.params.billing._meter_list_event_summaries_params import (
        MeterListEventSummariesParams,
    )
    from stripe.params.billing._meter_list_params import MeterListParams
    from stripe.params.billing._meter_modify_params import MeterModifyParams
    from stripe.params.billing._meter_reactivate_params import (
        MeterReactivateParams,
    )
    from stripe.params.billing._meter_retrieve_params import (
        MeterRetrieveParams,
    )


@nested_resource_class_methods("event_summary")
class Meter(
    CreateableAPIResource["Meter"],
    ListableAPIResource["Meter"],
    UpdateableAPIResource["Meter"],
):
    """
    Meters specify how to aggregate meter events over a billing period. Meter events represent the actions that customers take in your system. Meters attach to prices and form the basis of the bill.

    Related guide: [Usage based billing](https://docs.stripe.com/billing/subscriptions/usage-based)
    """

    OBJECT_NAME: ClassVar[Literal["billing.meter"]] = "billing.meter"

    class CustomerMapping(StripeObject):
        event_payload_key: str
        """
        The key in the meter event payload to use for mapping the event to a customer.
        """
        type: Literal["by_id"]
        """
        The method for mapping a meter event to a customer.
        """

    class DefaultAggregation(StripeObject):
        formula: Literal["count", "last", "sum"]
        """
        Specifies how events are aggregated.
        """

    class StatusTransitions(StripeObject):
        deactivated_at: Optional[int]
        """
        The time the meter was deactivated, if any. Measured in seconds since Unix epoch.
        """

    class ValueSettings(StripeObject):
        event_payload_key: str
        """
        The key in the meter event payload to use as the value for this meter.
        """

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer_mapping: CustomerMapping
    default_aggregation: DefaultAggregation
    display_name: str
    """
    The meter's name.
    """
    event_name: str
    """
    The name of the meter event to record usage for. Corresponds with the `event_name` field on meter events.
    """
    event_time_window: Optional[Literal["day", "hour"]]
    """
    The time window which meter events have been pre-aggregated for, if any.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["billing.meter"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Literal["active", "inactive"]
    """
    The meter's status.
    """
    status_transitions: StatusTransitions
    updated: int
    """
    Time at which the object was last updated. Measured in seconds since the Unix epoch.
    """
    value_settings: ValueSettings

    @classmethod
    def create(cls, **params: Unpack["MeterCreateParams"]) -> "Meter":
        """
        Creates a billing meter.
        """
        return cast(
            "Meter",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["MeterCreateParams"]
    ) -> "Meter":
        """
        Creates a billing meter.
        """
        return cast(
            "Meter",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def _cls_deactivate(
        cls, id: str, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        return cast(
            "Meter",
            cls._static_request(
                "post",
                "/v1/billing/meters/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def deactivate(
        id: str, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        ...

    @overload
    def deactivate(self, **params: Unpack["MeterDeactivateParams"]) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        ...

    @class_method_variant("_cls_deactivate")
    def deactivate(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        return cast(
            "Meter",
            self._request(
                "post",
                "/v1/billing/meters/{id}/deactivate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_deactivate_async(
        cls, id: str, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        return cast(
            "Meter",
            await cls._static_request_async(
                "post",
                "/v1/billing/meters/{id}/deactivate".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def deactivate_async(
        id: str, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        ...

    @overload
    async def deactivate_async(
        self, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        ...

    @class_method_variant("_cls_deactivate_async")
    async def deactivate_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["MeterDeactivateParams"]
    ) -> "Meter":
        """
        When a meter is deactivated, no more meter events will be accepted for this meter. You can't attach a deactivated meter to a price.
        """
        return cast(
            "Meter",
            await self._request_async(
                "post",
                "/v1/billing/meters/{id}/deactivate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["MeterListParams"]) -> ListObject["Meter"]:
        """
        Retrieve a list of billing meters.
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
        cls, **params: Unpack["MeterListParams"]
    ) -> ListObject["Meter"]:
        """
        Retrieve a list of billing meters.
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
    def modify(cls, id: str, **params: Unpack["MeterModifyParams"]) -> "Meter":
        """
        Updates a billing meter.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Meter",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["MeterModifyParams"]
    ) -> "Meter":
        """
        Updates a billing meter.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Meter",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def _cls_reactivate(
        cls, id: str, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        return cast(
            "Meter",
            cls._static_request(
                "post",
                "/v1/billing/meters/{id}/reactivate".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def reactivate(
        id: str, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        ...

    @overload
    def reactivate(self, **params: Unpack["MeterReactivateParams"]) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        ...

    @class_method_variant("_cls_reactivate")
    def reactivate(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        return cast(
            "Meter",
            self._request(
                "post",
                "/v1/billing/meters/{id}/reactivate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_reactivate_async(
        cls, id: str, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        return cast(
            "Meter",
            await cls._static_request_async(
                "post",
                "/v1/billing/meters/{id}/reactivate".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def reactivate_async(
        id: str, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        ...

    @overload
    async def reactivate_async(
        self, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        ...

    @class_method_variant("_cls_reactivate_async")
    async def reactivate_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["MeterReactivateParams"]
    ) -> "Meter":
        """
        When a meter is reactivated, events for this meter can be accepted and you can attach the meter to a price.
        """
        return cast(
            "Meter",
            await self._request_async(
                "post",
                "/v1/billing/meters/{id}/reactivate".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["MeterRetrieveParams"]
    ) -> "Meter":
        """
        Retrieves a billing meter given an ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["MeterRetrieveParams"]
    ) -> "Meter":
        """
        Retrieves a billing meter given an ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def list_event_summaries(
        cls, id: str, **params: Unpack["MeterListEventSummariesParams"]
    ) -> ListObject["MeterEventSummary"]:
        """
        Retrieve a list of billing meter event summaries.
        """
        return cast(
            ListObject["MeterEventSummary"],
            cls._static_request(
                "get",
                "/v1/billing/meters/{id}/event_summaries".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_event_summaries_async(
        cls, id: str, **params: Unpack["MeterListEventSummariesParams"]
    ) -> ListObject["MeterEventSummary"]:
        """
        Retrieve a list of billing meter event summaries.
        """
        return cast(
            ListObject["MeterEventSummary"],
            await cls._static_request_async(
                "get",
                "/v1/billing/meters/{id}/event_summaries".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "customer_mapping": CustomerMapping,
        "default_aggregation": DefaultAggregation,
        "status_transitions": StatusTransitions,
        "value_settings": ValueSettings,
    }
