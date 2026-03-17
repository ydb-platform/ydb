from typing_extensions import TYPE_CHECKING
from stripe.v2._list_object import ListObject as ListObject
from stripe.v2._amount import Amount as Amount, AmountParam as AmountParam


# The beginning of the section generated from our OpenAPI spec
from importlib import import_module

if TYPE_CHECKING:
    from stripe.v2 import billing as billing, core as core
    from stripe.v2._billing_service import BillingService as BillingService
    from stripe.v2._core_service import CoreService as CoreService
    from stripe.v2._deleted_object import DeletedObject as DeletedObject

# name -> (import_target, is_submodule)
_import_map = {
    "billing": ("stripe.v2.billing", True),
    "core": ("stripe.v2.core", True),
    "BillingService": ("stripe.v2._billing_service", False),
    "CoreService": ("stripe.v2._core_service", False),
    "DeletedObject": ("stripe.v2._deleted_object", False),
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

# The end of the section generated from our OpenAPI spec
