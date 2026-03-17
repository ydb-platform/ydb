class TeamsAsyncOperationStatus:
    def __init__(self):
        """Describes the current status of a teamsAsyncOperation."""
        pass

    invalid = 0
    """	Invalid value."""

    notStarted = 1
    """The operation has not started."""

    inProgress = 2
    """	The operation is running."""

    succeeded = 3
    """The operation succeeded."""

    failed = 4
    """The operation failed."""
