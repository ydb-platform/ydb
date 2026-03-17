from datetime import datetime
from typing import Optional

from office365.entity import Entity


class DataPolicyOperation(Entity):
    """
    Represents a submitted data policy operation. It contains necessary information for tracking the status of
    an operation. For example, a company administrator can submit a data policy operation request to export an
    employee's company data, and then later track that request.
    """

    @property
    def completed_datetime(self):
        # type: () -> datetime
        """Represents when the request for this data policy operation was completed"""
        return self.properties.get("completedDateTime", datetime.min)

    @property
    def progress(self):
        # type: () -> Optional[float]
        """Specifies the progress of an operation."""
        return self.properties.get("progress", None)

    @property
    def status(self):
        # type: () -> Optional[str]
        """Possible values are: notStarted, running, complete, failed, unknownFutureValue."""
        return self.properties.get("status", None)

    @property
    def storage_location(self):
        # type: () -> Optional[str]
        """The URL location to where data is being exported for export requests."""
        return self.properties.get("storageLocation", None)

    @property
    def submitted_datetime(self):
        # type: () -> datetime
        """Represents when the request for this data operation was submitted"""
        return self.properties.get("submittedDateTime", datetime.min)

    @property
    def user_id(self):
        # type: () -> Optional[str]
        """The id for the user on whom the operation is performed."""
        return self.properties.get("userId", None)
