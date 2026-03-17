from typing import Optional

from office365.directory.licenses.service_plan_info import ServicePlanInfo
from office365.directory.licenses.units_detail import LicenseUnitsDetail
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection


class SubscribedSku(Entity):
    """Contains information about a service SKU that a company is subscribed to."""

    @property
    def account_id(self):
        # type: () -> Optional[str]
        """The unique ID of the account this SKU belongs to."""
        return self.properties.get("accountId", None)

    @property
    def applies_to(self):
        # type: () -> Optional[str]
        """
        The target class for this SKU. Only SKUs with target class User are assignable.
        Possible values are: "User", "Company".
        """
        return self.properties.get("appliesTo", None)

    @property
    def sku_id(self):
        # type: () -> Optional[str]
        """The unique identifier (GUID) for the service SKU."""
        return self.properties.get("skuId", None)

    @property
    def sku_part_number(self):
        # type: () -> Optional[str]
        """
        The SKU part number; for example: "AAD_PREMIUM" or "RMSBASIC".
        To get a list of commercial subscriptions that an organization has acquired, see List subscribedSkus.
        """
        return self.properties.get("skuPartNumber", None)

    @property
    def prepaid_units(self):
        """Information about the number and status of prepaid licenses."""
        return self.properties.get("prepaidUnits", LicenseUnitsDetail())

    @property
    def service_plans(self):
        """Information about the service plans that are available with the SKU. Not nullable"""
        return self.properties.get(
            "servicePlans", ClientValueCollection(ServicePlanInfo)
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "prepaidUnits": self.prepaid_units,
                "servicePlans": self.service_plans,
            }
            default_value = property_mapping.get(name, None)
        return super(SubscribedSku, self).get_property(name, default_value)
