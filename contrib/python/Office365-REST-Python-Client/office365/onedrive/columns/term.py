from office365.runtime.client_value import ClientValue


class TermColumn(ClientValue):
    """Represents a managed metadata column in SharePoint."""

    def __init__(self, allow_multiple_values=None, show_fully_qualified_name=None):
        """
        :param bool allow_multiple_values: Specifies whether the column will allow more than one value.
        :param bool show_fully_qualified_name: Specifies whether to display the entire term path or only the term label.
        """
        super(TermColumn, self).__init__()
        self.allowMultipleValues = allow_multiple_values
        self.showFullyQualifiedName = show_fully_qualified_name
