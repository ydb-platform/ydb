from office365.directory.synchronization.error import SynchronizationError
from office365.runtime.client_value import ClientValue


class SynchronizationQuarantine(ClientValue):
    """Provides information about the quarantine state of a synchronizationJob."""

    def __init__(self, error=SynchronizationError()):
        """
        :param SynchronizationError error: Describes the error(s) that occurred when putting the synchronization job
            into quarantine.
        """
        self.error = error
