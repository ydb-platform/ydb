# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.climate._order import Order as Order
    from stripe.climate._order_service import OrderService as OrderService
    from stripe.climate._product import Product as Product
    from stripe.climate._product_service import (
        ProductService as ProductService,
    )
    from stripe.climate._supplier import Supplier as Supplier
    from stripe.climate._supplier_service import (
        SupplierService as SupplierService,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "Order": ("stripe.climate._order", False),
    "OrderService": ("stripe.climate._order_service", False),
    "Product": ("stripe.climate._product", False),
    "ProductService": ("stripe.climate._product_service", False),
    "Supplier": ("stripe.climate._supplier", False),
    "SupplierService": ("stripe.climate._supplier_service", False),
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
