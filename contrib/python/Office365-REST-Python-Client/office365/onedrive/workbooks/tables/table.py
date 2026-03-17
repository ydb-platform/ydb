from typing import Optional

from office365.entity import Entity
from office365.onedrive.workbooks.ranges.range import WorkbookRange
from office365.onedrive.workbooks.tables.columns.collection import (
    WorkbookTableColumnCollection,
)
from office365.onedrive.workbooks.tables.rows.collection import (
    WorkbookTableRowCollection,
)
from office365.onedrive.workbooks.tables.sort import WorkbookTableSort
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookTable(Entity):
    """Represents an Excel table."""

    def __repr__(self):
        return self.name or self.id or self.entity_type_name

    def data_body_range(self):
        """Gets the range object associated with the data body of the table."""
        return_type = WorkbookRange(
            self.context, ResourcePath("dataBodyRange", self.resource_path)
        )
        qry = FunctionQuery(self, "dataBodyRange", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    def range(self):
        """Get the range object associated with the entire table."""
        return_type = WorkbookRange(
            self.context, ResourcePath("range", self.resource_path)
        )
        qry = FunctionQuery(self, "range", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    def total_row_range(self):
        """Gets the range object associated with totals row of the table."""
        return_type = WorkbookRange(self.context)
        qry = FunctionQuery(self, "totalRowRange", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    def clear_filters(self):
        """Clears all the filters currently applied on the table."""
        qry = ServiceOperationQuery(self, "clearFilters")
        self.context.add_query(qry)
        return self

    def reapply_filters(self):
        """Reapplies all the filters currently on the table."""
        qry = ServiceOperationQuery(self, "reapplyFilters")
        self.context.add_query(qry)
        return self

    @property
    def highlight_first_column(self):
        # type: () -> Optional[bool]
        """Indicates whether the first column contains special formatting."""
        return self.properties.get("highlightFirstColumn", None)

    @property
    def highlight_last_column(self):
        # type: () -> Optional[bool]
        """Indicates whether the last column contains special formatting."""
        return self.properties.get("highlightLastColumn", None)

    @property
    def legacy_id(self):
        # type: () -> Optional[str]
        """Legacy Id used in older Excel clients. The value of the identifier remains the same even when the table
        is renamed. This property should be interpreted as an opaque string value and shouldn't be parsed to any
        other type."""
        return self.properties.get("legacyId", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Name of the table."""
        return self.properties.get("name", None)

    @property
    def show_banded_columns(self):
        # type: () -> Optional[bool]
        """Indicates whether the columns show banded formatting in which odd columns are highlighted differently
        from even ones to make reading the table easier."""
        return self.properties.get("showBandedColumns", None)

    @property
    def show_banded_rows(self):
        # type: () -> Optional[bool]
        """Indicates whether the rows show banded formatting in which odd rows are highlighted differently from even
        ones to make reading the table easier."""
        return self.properties.get("showBandedRows", None)

    @property
    def show_filter_button(self):
        # type: () -> Optional[bool]
        """Indicates whether the filter buttons are visible at the top of each column header. Setting this is only
        allowed if the table contains a header row."""
        return self.properties.get("showFilterButton", None)

    @property
    def show_headers(self):
        # type: () -> Optional[bool]
        """Indicates whether the header row is visible or not. This value can be set to show or remove the header row"""
        return self.properties.get("showHeaders", None)

    @property
    def columns(self):
        # type: () -> WorkbookTableColumnCollection
        """Represents a collection of all the columns in the table."""
        return self.properties.get(
            "columns",
            WorkbookTableColumnCollection(
                self.context, ResourcePath("columns", self.resource_path)
            ),
        )

    @property
    def rows(self):
        # type: () -> WorkbookTableRowCollection
        """Represents a collection of all the rows in the table."""
        return self.properties.get(
            "rows",
            WorkbookTableRowCollection(
                self.context, ResourcePath("rows", self.resource_path)
            ),
        )

    @property
    def sort(self):
        # type: () -> WorkbookTableSort
        """Represents the sorting for the table."""
        return self.properties.get(
            "sort",
            WorkbookTableSort(self.context, ResourcePath("sort", self.resource_path)),
        )

    @property
    def worksheet(self):
        """The worksheet containing the current table."""
        from office365.onedrive.workbooks.worksheets.worksheet import WorkbookWorksheet

        return self.properties.get(
            "worksheet",
            WorkbookWorksheet(
                self.context, ResourcePath("worksheet", self.resource_path)
            ),
        )
