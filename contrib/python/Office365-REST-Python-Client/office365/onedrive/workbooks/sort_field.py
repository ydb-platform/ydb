from office365.onedrive.workbooks.icon import WorkbookIcon
from office365.runtime.client_value import ClientValue


class WorkbookSortField(ClientValue):
    """Represents a condition in a sorting operation."""

    def __init__(
        self,
        ascending=None,
        color=None,
        data_option=None,
        icon=WorkbookIcon(),
        key=None,
    ):
        """
        :param bool ascending: Represents whether the sorting is done in an ascending fashion.
        :param str color: Represents the color that is the target of the condition if the sorting is on font
             or cell color.
        :param str data_option: Represents additional sorting options for this field.
             Possible values are: Normal, TextAsNumber.
        :param WorkbookIcon icon: Represents the icon that is the target of the condition if the sorting
             is on the cell's icon.
        :param str key: Represents the column (or row, depending on the sort orientation) that the condition is on.
              Represented as an offset from the first column (or row).
        """
        self.ascending = ascending
        self.color = color
        self.dataOption = data_option
        self.icon = icon
        self.key = key
