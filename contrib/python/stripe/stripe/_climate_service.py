# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.climate._order_service import OrderService
    from stripe.climate._product_service import ProductService
    from stripe.climate._supplier_service import SupplierService

_subservices = {
    "orders": ["stripe.climate._order_service", "OrderService"],
    "products": ["stripe.climate._product_service", "ProductService"],
    "suppliers": ["stripe.climate._supplier_service", "SupplierService"],
}


class ClimateService(StripeService):
    orders: "OrderService"
    products: "ProductService"
    suppliers: "SupplierService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()
