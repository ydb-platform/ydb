# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.reporting._report_run import ReportRun as ReportRun
    from stripe.reporting._report_run_service import (
        ReportRunService as ReportRunService,
    )
    from stripe.reporting._report_type import ReportType as ReportType
    from stripe.reporting._report_type_service import (
        ReportTypeService as ReportTypeService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ReportRun": ("stripe.reporting._report_run", False),
    "ReportRunService": ("stripe.reporting._report_run_service", False),
    "ReportType": ("stripe.reporting._report_type", False),
    "ReportTypeService": ("stripe.reporting._report_type_service", False),
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
