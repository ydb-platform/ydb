from office365.runtime.client_value import ClientValue


class WorkbookOperationError(ClientValue):
    """Represents an error from a failed workbook operation."""

    def __init__(self, code=None, innerError=None, message=None):
        """
        :param str code:
        :param str innerError:
        :param str message:
        """
        self.code = code
        self.innerError = innerError
        self.message = message
