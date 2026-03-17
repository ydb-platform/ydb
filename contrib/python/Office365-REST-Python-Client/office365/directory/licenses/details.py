from typing import Optional

from office365.directory.licenses.service_plan_info import ServicePlanInfo
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection


class LicenseDetails(Entity):
    """Contains information about a license assigned to a user."""

    def __repr__(self):
        return self.sku_part_number or self.entity_type_name

    @property
    def service_plans(self):
        """Information about the service plans assigned with the license. Read-only, Not nullable"""
        return self.properties.get(
            "servicePlans", ClientValueCollection(ServicePlanInfo)
        )

    @property
    def sku_id(self):
        # type: () -> Optional[str]
        """
        Unique identifier (GUID) for the service SKU. Equal to the skuId property on the related SubscribedSku object.
        """
        return self.properties.get("skuId", None)

    @property
    def sku_part_number(self):
        # type: () -> Optional[str]
        """
        Unique SKU display name. Equal to the skuPartNumber on the related SubscribedSku object;
        for example: "AAD_Premium". Read-only
        """
        return self.properties.get("skuPartNumber", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"servicePlans": self.service_plans}
            default_value = property_mapping.get(name, None)
        return super(LicenseDetails, self).get_property(name, default_value)
