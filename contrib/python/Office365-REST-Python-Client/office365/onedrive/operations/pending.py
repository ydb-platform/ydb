from office365.onedrive.operations.pending_content_update import PendingContentUpdate
from office365.runtime.client_value import ClientValue


class PendingOperations(ClientValue):
    """Indicates that one or more operations that might affect the state of the driveItem are pending completion."""

    def __init__(self, pending_content_update=PendingContentUpdate()):
        """
        :param str pending_content_update: A property that indicates that an operation that might update the binary
            content of a file is pending completion.
        """
        self.pendingContentUpdate = pending_content_update
