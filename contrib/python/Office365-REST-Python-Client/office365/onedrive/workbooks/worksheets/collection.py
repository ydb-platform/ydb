from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.worksheets.worksheet import WorkbookWorksheet
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookWorksheetCollection(EntityCollection[WorkbookWorksheet]):
    def __init__(self, context, resource_path=None):
        super(WorkbookWorksheetCollection, self).__init__(
            context, WorkbookWorksheet, resource_path
        )

    def add(self, name=None):
        """
        Adds a new worksheet to the workbook. The worksheet will be added at the end of existing worksheets.
        If you wish to activate the newly added worksheet, call ".activate() on it.

        :param str name: The name of the worksheet to be added. If specified, name should be unique.
            If not specified, Excel determines the name of the new worksheet.
        """
        return_type = WorkbookWorksheet(self.context)
        self.add_child(return_type)
        payload = {"name": name}
        qry = ServiceOperationQuery(self, "add", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type
