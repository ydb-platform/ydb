# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.climate._order_cancel_params import (
        OrderCancelParams as OrderCancelParams,
    )
    from stripe.params.climate._order_create_params import (
        OrderCreateParams as OrderCreateParams,
        OrderCreateParamsBeneficiary as OrderCreateParamsBeneficiary,
    )
    from stripe.params.climate._order_list_params import (
        OrderListParams as OrderListParams,
    )
    from stripe.params.climate._order_modify_params import (
        OrderModifyParams as OrderModifyParams,
        OrderModifyParamsBeneficiary as OrderModifyParamsBeneficiary,
    )
    from stripe.params.climate._order_retrieve_params import (
        OrderRetrieveParams as OrderRetrieveParams,
    )
    from stripe.params.climate._order_update_params import (
        OrderUpdateParams as OrderUpdateParams,
        OrderUpdateParamsBeneficiary as OrderUpdateParamsBeneficiary,
    )
    from stripe.params.climate._product_list_params import (
        ProductListParams as ProductListParams,
    )
    from stripe.params.climate._product_retrieve_params import (
        ProductRetrieveParams as ProductRetrieveParams,
    )
    from stripe.params.climate._supplier_list_params import (
        SupplierListParams as SupplierListParams,
    )
    from stripe.params.climate._supplier_retrieve_params import (
        SupplierRetrieveParams as SupplierRetrieveParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "OrderCancelParams": ("stripe.params.climate._order_cancel_params", False),
    "OrderCreateParams": ("stripe.params.climate._order_create_params", False),
    "OrderCreateParamsBeneficiary": (
        "stripe.params.climate._order_create_params",
        False,
    ),
    "OrderListParams": ("stripe.params.climate._order_list_params", False),
    "OrderModifyParams": ("stripe.params.climate._order_modify_params", False),
    "OrderModifyParamsBeneficiary": (
        "stripe.params.climate._order_modify_params",
        False,
    ),
    "OrderRetrieveParams": (
        "stripe.params.climate._order_retrieve_params",
        False,
    ),
    "OrderUpdateParams": ("stripe.params.climate._order_update_params", False),
    "OrderUpdateParamsBeneficiary": (
        "stripe.params.climate._order_update_params",
        False,
    ),
    "ProductListParams": ("stripe.params.climate._product_list_params", False),
    "ProductRetrieveParams": (
        "stripe.params.climate._product_retrieve_params",
        False,
    ),
    "SupplierListParams": (
        "stripe.params.climate._supplier_list_params",
        False,
    ),
    "SupplierRetrieveParams": (
        "stripe.params.climate._supplier_retrieve_params",
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
