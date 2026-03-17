from typing import Optional

from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.charts.chart import WorkbookChart
from office365.onedrive.workbooks.names.named_item import WorkbookNamedItem
from office365.onedrive.workbooks.ranges.range import WorkbookRange
from office365.onedrive.workbooks.tables.collection import WorkbookTableCollection
from office365.onedrive.workbooks.tables.pivot_table_collection import (
    WorkbookPivotTableCollection,
)
from office365.onedrive.workbooks.worksheets.protection import (
    WorkbookWorksheetProtection,
)
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery


class WorkbookWorksheet(Entity):
    """
    An Excel worksheet is a grid of cells. It can contain data, tables, charts, etc
    """

    def __repr__(self):
        return self.name or self.entity_type_name

    def __str__(self):
        return self.name or self.entity_type_name

    def cell(self, row, column):
        """Gets the range object containing the single cell based on row and column numbers.
        The cell can be outside the bounds of its parent range, so long as it's stays within the worksheet grid.
        :param int row: Row number of the cell to be retrieved. Zero-indexed.
        :param int column: Column number of the cell to be retrieved. Zero-indexed.
        """
        return_type = WorkbookRange(
            self.context, ResourcePath("range", self.resource_path)
        )
        params = {"row": row, "column": column}
        qry = FunctionQuery(self, "cell", method_params=params, return_type=return_type)
        self.context.add_query(qry)
        return return_type

    def range(self, address=None):
        """Gets the range object specified by the address or name."""
        return_type = WorkbookRange(
            self.context, ResourcePath("range", self.resource_path)
        )
        params = {"address": address}
        qry = FunctionQuery(
            self, "range", method_params=params, return_type=return_type
        )
        self.context.add_query(qry)
        return return_type

    def used_range(self, values_only=False):
        """Return the used range of the given range object.

        :param bool values_only: Optional. Considers only cells with values as used cells.
        """
        return_type = WorkbookRange(self.context)
        params = {"valuesOnly": values_only}
        qry = FunctionQuery(self, "usedRange", params, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def charts(self):
        # type: () -> EntityCollection[WorkbookChart]
        """Returns collection of charts that are part of the worksheet"""
        return self.properties.get(
            "charts",
            EntityCollection(
                self.context, WorkbookChart, ResourcePath("charts", self.resource_path)
            ),
        )

    @property
    def name(self):
        # type: () -> Optional[str]
        """The display name of the worksheet."""
        return self.properties.get("name", None)

    @property
    def names(self):
        # type: () -> EntityCollection[WorkbookNamedItem]
        """Returns collection of names that are associated with the worksheet"""
        return self.properties.get(
            "names",
            EntityCollection(
                self.context,
                WorkbookNamedItem,
                ResourcePath("names", self.resource_path),
            ),
        )

    @property
    def tables(self):
        # type: () -> WorkbookTableCollection
        """Collection of tables that are part of the worksheet."""
        return self.properties.get(
            "tables",
            WorkbookTableCollection(
                self.context, ResourcePath("tables", self.resource_path)
            ),
        )

    @property
    def pivot_tables(self):
        # type: () -> WorkbookPivotTableCollection
        """Collection of PivotTables that are part of the worksheet."""
        return self.properties.get(
            "pivotTables",
            WorkbookPivotTableCollection(
                self.context,
                ResourcePath("pivotTables", self.resource_path),
            ),
        )

    @property
    def protection(self):
        # type: () -> WorkbookWorksheetProtection
        """Returns sheet protection object for a worksheet."""
        return self.properties.get(
            "protection",
            WorkbookWorksheetProtection(
                self.context, ResourcePath("protection", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "pivotTables": self.pivot_tables,
            }
            default_value = property_mapping.get(name, None)
        return super(WorkbookWorksheet, self).get_property(name, default_value)
