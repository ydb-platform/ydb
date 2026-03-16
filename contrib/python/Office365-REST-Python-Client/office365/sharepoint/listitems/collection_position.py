from office365.runtime.client_value import ClientValue


class ListItemCollectionPosition(ClientValue):
    """Specifies a collection of list items."""

    def __init__(self, paging_info=None):
        super(ListItemCollectionPosition, self).__init__()
        self.PagingInfo = paging_info

    @property
    def entity_type_name(self):
        return "SP.ListItemCollectionPosition"
