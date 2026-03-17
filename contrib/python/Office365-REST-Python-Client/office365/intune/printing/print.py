from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.intune.printing.connectors.connector import PrintConnector
from office365.intune.printing.share import PrinterShare
from office365.runtime.paths.resource_path import ResourcePath


class Print(Entity):
    """When accompanied by a Universal Print subscription, the Print feature enables management of printers and
    discovery of printServiceEndpoints that can be used to manage printers and print jobs within Universal Print.
    """

    @property
    def connectors(self):
        """The list of available print connectors."""
        return self.properties.get(
            "connectors",
            EntityCollection(
                self.context,
                PrintConnector,
                ResourcePath("connectors", self.resource_path),
            ),
        )

    @property
    def shares(self):
        """The list of printer shares registered in the tenant."""
        return self.properties.get(
            "shares",
            EntityCollection(
                self.context, PrinterShare, ResourcePath("shares", self.resource_path)
            ),
        )
