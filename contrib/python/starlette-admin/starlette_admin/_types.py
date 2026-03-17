from enum import Enum


class RequestAction(str, Enum):
    """Enumeration of string constants that represent different types of actions that can be performed on a ModelView.

    Attributes:
        API: Used for select2 requests, which are requests for data to populate a select2 input field.
        LIST: Used for listing objects in a view (datatable requests).
        DETAIL: Used for displaying the details of a single object in a view.
        CREATE: Used for displaying a form for creating a new object in a view.
        EDIT: Used for displaying a form for editing an existing object in a view.
    """

    API = "API"
    LIST = "LIST"
    DETAIL = "DETAIL"
    CREATE = "CREATE"
    EDIT = "EDIT"
    ACTION = "ACTION"
    ROW_ACTION = "ROW_ACTION"

    def is_form(self) -> bool:
        return self.value in [self.CREATE, self.EDIT]


class ExportType(str, Enum):
    """Enumeration of string constants that represent different export types."""

    CSV = "csv"
    EXCEL = "excel"
    PDF = "pdf"
    PRINT = "print"


class RowActionsDisplayType(str, Enum):
    ICON_LIST = "ICON_LIST"
    DROPDOWN = "DROPDOWN"
