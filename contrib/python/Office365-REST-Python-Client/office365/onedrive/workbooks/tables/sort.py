from office365.entity import Entity
from office365.onedrive.workbooks.sort_field import WorkbookSortField
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookTableSort(Entity):
    """Manages sorting operations on Table objects."""

    def apply(self, fields, match_case=None, method=None):
        """Perform a sort operation.

        :param list[WorkbookSortField] fields: The list of conditions to sort on.
        :param bool match_case: Indicates whether to match the case of the items being sorted.
        :param str method: The ordering method used for Chinese characters.
             The possible values are: PinYin, StrokeCount.
        """
        payload = {
            "fields": ClientValueCollection(WorkbookSortField, fields),
            "matchCase": match_case,
            "method": method,
        }
        qry = ServiceOperationQuery(self, "apply", None, payload)
        self.context.add_query(qry)
        return self
