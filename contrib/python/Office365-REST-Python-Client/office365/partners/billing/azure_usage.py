from office365.entity import Entity
from office365.partners.billing.billed_usage import BilledUsage
from office365.runtime.paths.resource_path import ResourcePath


class AzureUsage(Entity):
    """Represents details for billed and unbilled Azure usage data."""

    @property
    def billed(self):
        """
        Represents details for billed Azure usage data.
        """
        return self.properties.get(
            "billed",
            BilledUsage(self.context, ResourcePath("billed", self.resource_path)),
        )
