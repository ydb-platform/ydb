class ImportExportError(Exception):
    """A generic exception for all others to extend."""

    pass


class FieldError(ImportExportError):
    """Raised when a field encounters an error."""

    pass


class WidgetError(ImportExportError):
    """Raised when there is a misconfiguration with a Widget."""

    pass


class ImportError(ImportExportError):
    def __init__(self, error, number=None, row=None):
        """A wrapper for errors thrown from the import process.

        :param error: The underlying error that occurred.
        :param number: The row number of the row containing the error (if obtainable).
        :param row: The row containing the error (if obtainable).
        """
        self.error = error
        self.number = number
        self.row = row

    def __str__(self):
        s = ""
        if self.number is not None:
            s += f"{self.number}: "
        s += f"{self.error}"
        if self.row is not None:
            s += f" ({self.row})"
        return s
