from office365.entity import Entity
from office365.onedrive.workbooks.filter import WorkbookFilter
from office365.onedrive.workbooks.ranges.range import WorkbookRange
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery


class WorkbookTableColumn(Entity):
    """Represents a column in a table."""

    def header_row_range(self):
        """
        Gets the range object associated with the header row of the column.
        """
        return_type = WorkbookRange(self.context)
        qry = FunctionQuery(self, "headerRowRange", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    def range(self):
        """
        Gets the range object associated with the entire column.
        """
        return_type = WorkbookRange(self.context)
        qry = FunctionQuery(self, "range", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def values(self):
        """
        Represents the raw values of the specified range. The data returned could be of type string, number,
        or a boolean. Cell that contain an error will return the error string.
        """
        return self.properties.get("values", None)

    @property
    def filter(self):
        """Retrieve the filter applied to the column."""
        return self.properties.get(
            "filter",
            WorkbookFilter(self.context, ResourcePath("filter", self.resource_path)),
        )
