from typing import Optional

from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookPivotTable(Entity):
    """Represents an Excel PivotTable."""

    def __str__(self):
        return self.name or self.entity_type_name

    def refresh(self):
        """Refreshes the PivotTable."""
        qry = ServiceOperationQuery(self, "refresh")
        self.context.add_query(qry)
        return self

    def refresh_all(self):
        """Refreshes the PivotTable within a given worksheet."""
        qry = ServiceOperationQuery(self, "refreshAll")
        self.context.add_query(qry)
        return self

    @property
    def name(self):
        # type: () -> Optional[str]
        """Name of the PivotTable."""
        return self.properties.get("Name", None)

    @property
    def worksheet(self):
        """The worksheet containing the current PivotTable"""
        from office365.onedrive.workbooks.worksheets.worksheet import WorkbookWorksheet

        return self.properties.get(
            "worksheet",
            WorkbookWorksheet(
                self.context, ResourcePath("worksheet", self.resource_path)
            ),
        )
