# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.entitlements._active_entitlement import (
        ActiveEntitlement as ActiveEntitlement,
    )
    from stripe.entitlements._active_entitlement_service import (
        ActiveEntitlementService as ActiveEntitlementService,
    )
    from stripe.entitlements._active_entitlement_summary import (
        ActiveEntitlementSummary as ActiveEntitlementSummary,
    )
    from stripe.entitlements._feature import Feature as Feature
    from stripe.entitlements._feature_service import (
        FeatureService as FeatureService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ActiveEntitlement": ("stripe.entitlements._active_entitlement", False),
    "ActiveEntitlementService": (
        "stripe.entitlements._active_entitlement_service",
        False,
    ),
    "ActiveEntitlementSummary": (
        "stripe.entitlements._active_entitlement_summary",
        False,
    ),
    "Feature": ("stripe.entitlements._feature", False),
    "FeatureService": ("stripe.entitlements._feature_service", False),
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
