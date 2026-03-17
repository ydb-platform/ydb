from typing import Optional

from office365.entity import Entity
from office365.onedrive.workbooks.operations.error import WorkbookOperationError


class WorkbookOperation(Entity):
    """Represents the status of a long-running workbook operation"""

    @property
    def error(self):
        """The error returned by the operation."""
        return self.properties.get("error", WorkbookOperationError())

    @property
    def resource_location(self):
        # type: () -> Optional[str]
        """The resource URI for the result."""
        return self.properties.get("resourceLocation", None)

    @property
    def status(self):
        # type: () -> Optional[str]
        """The current status of the operation. Possible values are: NotStarted, Running, Completed, Failed."""
        return self.properties.get("status", None)
