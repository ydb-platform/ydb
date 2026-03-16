from office365.runtime.client_value import ClientValue


class OnenoteOperationError(ClientValue):
    """An error from a failed OneNote operation."""

    def __init__(self, message=None, code=None):
        """
        :param str message: The error message.
        :param str code: The error code.
        """
        super(OnenoteOperationError, self).__init__()
        self.message = message
        self.code = code
