from office365.runtime.client_value import ClientValue
from office365.sharepoint.lists.collection_position import ListCollectionPosition


class GetListsParameters(ClientValue):
    def __init__(self, position=ListCollectionPosition(), row_limit=100):
        """
        :param ListCollectionPosition position:
        """
        self.ListCollectionPosition = position
        self.RowLimit = row_limit

    @property
    def entity_type_name(self):
        return "SP.GetListsParameters"
