from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.simple_data_row import SimpleDataRow


class SimpleDataTable(ClientValue):
    """Represents a data table"""

    def __init__(self, rows=None):
        """
        :param list[SimpleDataRow] rows: The rows in the data table.
        """
        self.Rows = ClientValueCollection(SimpleDataRow, rows)

    @property
    def entity_type_name(self):
        return "SP.SimpleDataTable"
