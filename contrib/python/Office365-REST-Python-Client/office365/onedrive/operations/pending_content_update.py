from office365.runtime.client_value import ClientValue


class PendingContentUpdate(ClientValue):
    """Indicates that an operation that might affect the binary content of the driveItem is pending completion."""

    def __init__(self, queued_datetime=None):
        """
        :param datetime.datetime queued_datetime: Date and time the pending binary operation was queued in UTC time.
        """
        self.queuedDateTime = queued_datetime
