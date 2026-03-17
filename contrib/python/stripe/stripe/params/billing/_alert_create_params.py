# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class AlertCreateParams(RequestOptions):
    alert_type: Literal["usage_threshold"]
    """
    The type of alert to create.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    title: str
    """
    The title of the alert.
    """
    usage_threshold: NotRequired["AlertCreateParamsUsageThreshold"]
    """
    The configuration of the usage threshold.
    """


class AlertCreateParamsUsageThreshold(TypedDict):
    filters: NotRequired[List["AlertCreateParamsUsageThresholdFilter"]]
    """
    The filters allows limiting the scope of this usage alert. You can only specify up to one filter at this time.
    """
    gte: int
    """
    Defines the threshold value that triggers the alert.
    """
    meter: str
    """
    The [Billing Meter](https://docs.stripe.com/api/billing/meter) ID whose usage is monitored.
    """
    recurrence: Literal["one_time"]
    """
    Defines how the alert will behave.
    """


class AlertCreateParamsUsageThresholdFilter(TypedDict):
    customer: NotRequired[str]
    """
    Limit the scope to this usage alert only to this customer.
    """
    type: Literal["customer"]
    """
    What type of filter is being applied to this usage alert.
    """
