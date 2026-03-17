from office365.onenote.operations.onenote_operation_error import OnenoteOperationError
from office365.onenote.operations.operation import Operation


class OnenoteOperation(Operation):
    """The status of certain long-running OneNote operations."""

    @property
    def error(self):
        """The error returned by the operation."""
        return self.properties.get("error", OnenoteOperationError())
