from office365.directory.permissions.email_identity import EmailIdentity
from office365.directory.security.attacksimulations.automation_run import (
    SimulationAutomationRun,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class SimulationAutomation(Entity):
    """Represents simulation automation created to run on a tenant."""

    @property
    def created_by(self):
        """Identity of the user who created the attack simulation automation."""
        return self.properties.get("createdBy", EmailIdentity())

    @property
    def runs(self):
        """A collection of simulation automation runs."""
        return self.properties.get(
            "runs",
            EntityCollection(
                self.context,
                SimulationAutomationRun,
                ResourcePath("runs", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
            }
            default_value = property_mapping.get(name, None)
        return super(SimulationAutomation, self).get_property(name, default_value)
