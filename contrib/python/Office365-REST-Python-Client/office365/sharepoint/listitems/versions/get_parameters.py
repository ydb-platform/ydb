from office365.runtime.client_value import ClientValue


class GetListItemVersionsParameters(ClientValue):
    """"""

    def __init__(self, row_limit=None, sort_descending=None):
        """
        :param int row_limit:
        :param bool sort_descending:
        """
        self.RowLimit = row_limit
        self.SortDescending = sort_descending
