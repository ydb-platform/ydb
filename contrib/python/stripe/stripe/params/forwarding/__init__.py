# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.forwarding._request_create_params import (
        RequestCreateParams as RequestCreateParams,
        RequestCreateParamsRequest as RequestCreateParamsRequest,
        RequestCreateParamsRequestHeader as RequestCreateParamsRequestHeader,
    )
    from stripe.params.forwarding._request_list_params import (
        RequestListParams as RequestListParams,
        RequestListParamsCreated as RequestListParamsCreated,
    )
    from stripe.params.forwarding._request_retrieve_params import (
        RequestRetrieveParams as RequestRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "RequestCreateParams": (
        "stripe.params.forwarding._request_create_params",
        False,
    ),
    "RequestCreateParamsRequest": (
        "stripe.params.forwarding._request_create_params",
        False,
    ),
    "RequestCreateParamsRequestHeader": (
        "stripe.params.forwarding._request_create_params",
        False,
    ),
    "RequestListParams": (
        "stripe.params.forwarding._request_list_params",
        False,
    ),
    "RequestListParamsCreated": (
        "stripe.params.forwarding._request_list_params",
        False,
    ),
    "RequestRetrieveParams": (
        "stripe.params.forwarding._request_retrieve_params",
        False,
    ),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()
