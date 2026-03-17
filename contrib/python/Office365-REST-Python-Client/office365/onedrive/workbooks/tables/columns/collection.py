from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.tables.columns.column import WorkbookTableColumn
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery


class WorkbookTableColumnCollection(EntityCollection[WorkbookTableColumn]):
    def __init__(self, context, resource_path=None):
        super(WorkbookTableColumnCollection, self).__init__(
            context, WorkbookTableColumn, resource_path
        )

    def add(self, index, name, values=None):
        """
        Adds a new column to the table.

        :param int index: Specifies the relative position of the new column. The previous column at this position
            is shifted to the right. The index value should be equal to or less than the last column's index value,
            so it cannot be used to append a column at the end of the table. Zero-indexed.
        :param list values: A 2-dimensional array of unformatted values of the table column.
        :param str name: Name
        """
        return super(WorkbookTableColumnCollection, self).add(
            index=index, values=values, name=name
        )

    def count(self):
        """"""
        return_type = ClientResult[int](self.context)
        qry = FunctionQuery(self, "count", None, return_type)
        self.context.add_query(qry)
        return return_type
