from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.names.named_item import WorkbookNamedItem
from office365.runtime.queries.service_operation import ServiceOperationQuery


class WorkbookNamedItemCollection(EntityCollection[WorkbookNamedItem]):
    def __init__(self, context, resource_path=None):
        super(WorkbookNamedItemCollection, self).__init__(
            context, WorkbookNamedItem, resource_path
        )

    def add(self, name, reference, comment=None):
        """
        Adds a new name to the collection of the given scope using the user's locale for the formula.

        :param str name: The name of the object.
        :param str reference: Represents the formula that the name is defined to refer to.
             For example, =Sheet14!$B$2:$H$12, =4.75,
        :param str comment: Represents the comment associated with this name.
        """
        return_type = WorkbookNamedItem(self.context)
        self.add_child(return_type)
        payload = {"name": name, "reference": reference, "comment": comment}
        qry = ServiceOperationQuery(self, "add", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type
