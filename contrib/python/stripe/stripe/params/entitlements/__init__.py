# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.entitlements._active_entitlement_list_params import (
        ActiveEntitlementListParams as ActiveEntitlementListParams,
    )
    from stripe.params.entitlements._active_entitlement_retrieve_params import (
        ActiveEntitlementRetrieveParams as ActiveEntitlementRetrieveParams,
    )
    from stripe.params.entitlements._feature_create_params import (
        FeatureCreateParams as FeatureCreateParams,
    )
    from stripe.params.entitlements._feature_list_params import (
        FeatureListParams as FeatureListParams,
    )
    from stripe.params.entitlements._feature_modify_params import (
        FeatureModifyParams as FeatureModifyParams,
    )
    from stripe.params.entitlements._feature_retrieve_params import (
        FeatureRetrieveParams as FeatureRetrieveParams,
    )
    from stripe.params.entitlements._feature_update_params import (
        FeatureUpdateParams as FeatureUpdateParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ActiveEntitlementListParams": (
        "stripe.params.entitlements._active_entitlement_list_params",
        False,
    ),
    "ActiveEntitlementRetrieveParams": (
        "stripe.params.entitlements._active_entitlement_retrieve_params",
        False,
    ),
    "FeatureCreateParams": (
        "stripe.params.entitlements._feature_create_params",
        False,
    ),
    "FeatureListParams": (
        "stripe.params.entitlements._feature_list_params",
        False,
    ),
    "FeatureModifyParams": (
        "stripe.params.entitlements._feature_modify_params",
        False,
    ),
    "FeatureRetrieveParams": (
        "stripe.params.entitlements._feature_retrieve_params",
        False,
    ),
    "FeatureUpdateParams": (
        "stripe.params.entitlements._feature_update_params",
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
