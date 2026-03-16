# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired


class ReaderListParams(RequestOptions):
    device_type: NotRequired[
        Literal[
            "bbpos_chipper2x",
            "bbpos_wisepad3",
            "bbpos_wisepos_e",
            "mobile_phone_reader",
            "simulated_stripe_s700",
            "simulated_stripe_s710",
            "simulated_wisepos_e",
            "stripe_m2",
            "stripe_s700",
            "stripe_s710",
            "verifone_P400",
        ]
    ]
    """
    Filters readers by device type
    """
    ending_before: NotRequired[str]
    """
    A cursor for use in pagination. `ending_before` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, starting with `obj_bar`, your subsequent call can include `ending_before=obj_bar` in order to fetch the previous page of the list.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    limit: NotRequired[int]
    """
    A limit on the number of objects to be returned. Limit can range between 1 and 100, and the default is 10.
    """
    location: NotRequired[str]
    """
    A location ID to filter the response list to only readers at the specific location
    """
    serial_number: NotRequired[str]
    """
    Filters readers by serial number
    """
    starting_after: NotRequired[str]
    """
    A cursor for use in pagination. `starting_after` is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with `obj_foo`, your subsequent call can include `starting_after=obj_foo` in order to fetch the next page of the list.
    """
    status: NotRequired[Literal["offline", "online"]]
    """
    A status filter to filter readers to only offline or online readers
    """
