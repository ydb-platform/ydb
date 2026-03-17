from office365.entity import Entity
from office365.onedrive.workbooks.worksheets.protection_options import (
    WorkbookWorksheetProtectionOptions,
)
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookWorksheetProtection(Entity):
    """Represents the protection of a sheet object."""

    def protect(self, options=None):
        """Protect a worksheet. It throws if the worksheet has been protected."""
        qry = ServiceOperationQuery(self, "protect", parameters_type=options)
        self.context.add_query(qry)
        return self

    def unprotect(self):
        """Unprotect a worksheet"""
        qry = ServiceOperationQuery(self, "unprotect")
        self.context.add_query(qry)
        return self

    @property
    def options(self):
        """ """
        return self.properties.get("options", WorkbookWorksheetProtectionOptions())
