from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.lists.template import ListTemplate


class ListTemplateCollection(EntityCollection[ListTemplate]):
    """Specifies a collection of list templates"""

    def __init__(self, context, resource_path=None):
        """Specifies a collection of list templates"""
        super(ListTemplateCollection, self).__init__(
            context, ListTemplate, resource_path
        )

    def get_by_name(self, name):
        """Returns the list template with the specified name.
        :param str name: The specified name.
        """
        return ListTemplate(
            self.context, ServiceOperationPath("getByName", [name], self.resource_path)
        )
