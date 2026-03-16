# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._api_mode import ApiMode
from stripe._stripe_object import StripeObject
from stripe._stripe_response import StripeResponse
from stripe.v2.core._event import Event, EventNotification
from typing import Any, Dict, List, Optional, cast
from typing_extensions import Literal, TYPE_CHECKING, override

if TYPE_CHECKING:
    from stripe._api_requestor import _APIRequestor


class V1BillingMeterNoMeterFoundEventNotification(EventNotification):
    LOOKUP_TYPE = "v1.billing.meter.no_meter_found"
    type: Literal["v1.billing.meter.no_meter_found"]

    @override
    def fetch_event(self) -> "V1BillingMeterNoMeterFoundEvent":
        return cast(
            "V1BillingMeterNoMeterFoundEvent",
            super().fetch_event(),
        )

    @override
    async def fetch_event_async(self) -> "V1BillingMeterNoMeterFoundEvent":
        return cast(
            "V1BillingMeterNoMeterFoundEvent",
            await super().fetch_event_async(),
        )


class V1BillingMeterNoMeterFoundEvent(Event):
    LOOKUP_TYPE = "v1.billing.meter.no_meter_found"
    type: Literal["v1.billing.meter.no_meter_found"]

    class V1BillingMeterNoMeterFoundEventData(StripeObject):
        class Reason(StripeObject):
            class ErrorType(StripeObject):
                class SampleError(StripeObject):
                    class Request(StripeObject):
                        identifier: str
                        """
                        The request idempotency key.
                        """

                    error_message: str
                    """
                    The error message.
                    """
                    request: Request
                    """
                    The request causes the error.
                    """
                    _inner_class_types = {"request": Request}

                code: Literal[
                    "archived_meter",
                    "meter_event_customer_not_found",
                    "meter_event_dimension_count_too_high",
                    "meter_event_invalid_value",
                    "meter_event_no_customer_defined",
                    "missing_dimension_payload_keys",
                    "no_meter",
                    "timestamp_in_future",
                    "timestamp_too_far_in_past",
                ]
                """
                Open Enum.
                """
                error_count: int
                """
                The number of errors of this type.
                """
                sample_errors: List[SampleError]
                """
                A list of sample errors of this type.
                """
                _inner_class_types = {"sample_errors": SampleError}

            error_count: int
            """
            The total error count within this window.
            """
            error_types: List[ErrorType]
            """
            The error details.
            """
            _inner_class_types = {"error_types": ErrorType}

        developer_message_summary: str
        """
        Extra field included in the event's `data` when fetched from /v2/events.
        """
        reason: Reason
        """
        This contains information about why meter error happens.
        """
        validation_end: str
        """
        The end of the window that is encapsulated by this summary.
        """
        validation_start: str
        """
        The start of the window that is encapsulated by this summary.
        """
        _inner_class_types = {"reason": Reason}

    data: V1BillingMeterNoMeterFoundEventData
    """
    Data for the v1.billing.meter.no_meter_found event
    """

    @classmethod
    def _construct_from(
        cls,
        *,
        values: Dict[str, Any],
        last_response: Optional[StripeResponse] = None,
        requestor: "_APIRequestor",
        api_mode: ApiMode,
    ) -> "V1BillingMeterNoMeterFoundEvent":
        evt = super()._construct_from(
            values=values,
            last_response=last_response,
            requestor=requestor,
            api_mode=api_mode,
        )
        if hasattr(evt, "data"):
            evt.data = V1BillingMeterNoMeterFoundEvent.V1BillingMeterNoMeterFoundEventData._construct_from(
                values=evt.data,
                last_response=last_response,
                requestor=requestor,
                api_mode=api_mode,
            )
        return evt
