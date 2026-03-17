# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.reporting._report_run_create_params import (
        ReportRunCreateParams as ReportRunCreateParams,
        ReportRunCreateParamsParameters as ReportRunCreateParamsParameters,
    )
    from stripe.params.reporting._report_run_list_params import (
        ReportRunListParams as ReportRunListParams,
        ReportRunListParamsCreated as ReportRunListParamsCreated,
    )
    from stripe.params.reporting._report_run_retrieve_params import (
        ReportRunRetrieveParams as ReportRunRetrieveParams,
    )
    from stripe.params.reporting._report_type_list_params import (
        ReportTypeListParams as ReportTypeListParams,
    )
    from stripe.params.reporting._report_type_retrieve_params import (
        ReportTypeRetrieveParams as ReportTypeRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ReportRunCreateParams": (
        "stripe.params.reporting._report_run_create_params",
        False,
    ),
    "ReportRunCreateParamsParameters": (
        "stripe.params.reporting._report_run_create_params",
        False,
    ),
    "ReportRunListParams": (
        "stripe.params.reporting._report_run_list_params",
        False,
    ),
    "ReportRunListParamsCreated": (
        "stripe.params.reporting._report_run_list_params",
        False,
    ),
    "ReportRunRetrieveParams": (
        "stripe.params.reporting._report_run_retrieve_params",
        False,
    ),
    "ReportTypeListParams": (
        "stripe.params.reporting._report_type_list_params",
        False,
    ),
    "ReportTypeRetrieveParams": (
        "stripe.params.reporting._report_type_retrieve_params",
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
