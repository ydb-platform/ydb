# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class ReceivedCreditListParams(RequestOptions):
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    financial_account: str
    """
    The FinancialAccount that received the funds.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    linked_flows: NotRequired["ReceivedCreditListParamsLinkedFlows"]
    """
    Only return ReceivedCredits described by the flow.
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["failed", "succeeded"]]
    """
    Only return ReceivedCredits that have the given status: `succeeded` or `failed`.
    """


class ReceivedCreditListParamsLinkedFlows(TypedDict):
    source_flow_type: Literal[
        "credit_reversal",
        "other",
        "outbound_payment",
        "outbound_transfer",
        "payout",
    ]
    """
    The source flow type.
    """
