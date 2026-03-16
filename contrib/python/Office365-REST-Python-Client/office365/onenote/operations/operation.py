from datetime import datetime

from office365.entity import Entity


class Operation(Entity):
    """The status of a long-running operation."""

    @property
    def created_datetime(self):
        """
        The start time of the operation.
        """
        return self.properties("createdDateTime", datetime.min)
