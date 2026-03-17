from office365.runtime.client_value import ClientValue


class WorkbookWorksheetProtectionOptions(ClientValue):
    """Represents the protection of a sheet object."""

    def __init__(
        self,
        allow_auto_filter=None,
        allow_delete_columns=None,
        allow_delete_rows=None,
        allow_format_cells=None,
        allow_format_columns=None,
        allow_format_rows=None,
        allow_insert_columns=None,
        allow_insert_hyperlinks=None,
        allow_insert_rows=None,
        allow_pivot_tables=None,
        allow_sort=None,
    ):
        """
        :param bool allow_auto_filter: Represents the worksheet protection option of allowing using auto filter feature.
        :param bool allow_delete_columns: Represents the worksheet protection option of allowing deleting columns.
        :param bool allow_delete_rows: Represents the worksheet protection option of allowing deleting rows.
        :param bool allow_format_cells: Represents the worksheet protection option of allowing formatting cells.
        :param bool allow_format_columns: Represents the worksheet protection option of allowing formatting columns.
        :param bool allow_format_rows: Represents the worksheet protection option of allowing formatting rows.
        :param bool allow_insert_columns: Represents the worksheet protection option of allowing inserting columns.
        :param bool allow_insert_hyperlinks: Represents the worksheet protection option of allowing inserting hyperlinks.
        :param bool allow_insert_rows: Represents the worksheet protection option of allowing inserting rows.
        :param bool allow_pivot_tables: Represents the worksheet protection option of allowing using pivot table feature.
        :param bool allow_sort: Represents the worksheet protection option of allowing sorting.
        """
        self.allowAutoFilter = allow_auto_filter
        self.allowDeleteColumns = allow_delete_columns
        self.allowDeleteRows = allow_delete_rows
        self.allowFormatCells = allow_format_cells
        self.allowFormatColumns = allow_format_columns
        self.allowFormatRows = allow_format_rows
        self.allowInsertColumns = allow_insert_columns
        self.allowInsertHyperlinks = allow_insert_hyperlinks
        self.allowInsertRows = allow_insert_rows
        self.allowPivotTables = allow_pivot_tables
        self.allowSort = allow_sort
