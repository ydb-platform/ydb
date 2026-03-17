from office365.entity import Entity
from office365.partners.billing.billing import Billing
from office365.runtime.paths.resource_path import ResourcePath


class Partners(Entity):
    """Represents billing details for a Microsoft direct partner."""

    @property
    def billing(self):
        """
        Represents billing details for billed and unbilled data.
        """
        return self.properties.get(
            "billing",
            Billing(self.context, ResourcePath("billing", self.resource_path)),
        )
