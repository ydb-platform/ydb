from office365.runtime.client_value import ClientValue


class WorkbookRangeReference(ClientValue):
    """"""

    def __init__(self, address=None):
        """
        :param str address:
        """
        self.address = address
