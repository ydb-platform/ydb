from office365.runtime.client_value import ClientValue
from office365.runtime.odata.type import ODataType


class SimpleDataRow(ClientValue):
    """Represents a row in a data table"""

    def __init__(self, cells=None):
        """
        :param dict cells: The cells in the data table row.
        """
        self.Cells = cells

    def set_property(self, k, v, persist_changes=True):
        self.Cells = ODataType.parse_key_value_collection(v)
        return self
