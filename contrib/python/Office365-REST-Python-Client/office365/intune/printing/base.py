from office365.entity import Entity
from office365.intune.printing.capabilities import PrinterCapabilities


class PrinterBase(Entity):
    """Represents a base type for printer and printerShare entity types."""

    @property
    def capabilities(self):
        """The capabilities of the printer/printerShare."""
        return self.properties.get("", PrinterCapabilities())
