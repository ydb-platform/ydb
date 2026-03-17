from office365.entity import Entity
from office365.partners.billing.azure_usage import AzureUsage
from office365.runtime.paths.resource_path import ResourcePath


class Billing(Entity):
    """Represents billing details for billed and unbilled data."""

    @property
    def usage(self):
        """
        Represents details for billed and unbilled Azure usage data.
        """
        return self.properties.get(
            "usage",
            AzureUsage(self.context, ResourcePath("usage", self.resource_path)),
        )
