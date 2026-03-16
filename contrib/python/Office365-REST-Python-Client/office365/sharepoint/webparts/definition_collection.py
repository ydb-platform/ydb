from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.webparts.definition import WebPartDefinition


class WebPartDefinitionCollection(EntityCollection[WebPartDefinition]):
    """Implements a collection of Web Part definition objects"""

    def __init__(self, context, resource_path=None):
        super(WebPartDefinitionCollection, self).__init__(
            context, WebPartDefinition, resource_path
        )

    def get_by_id(self, def_id):
        """
        Returns the Web Part definition object (1) in the collection with a Web Part identifier
        equal to the id parameter.

        :param str def_id: The Web Part identifier of the Web Part definition to retrieve.
        """
        return WebPartDefinition(
            self.context, ServiceOperationPath("GetById", [def_id], self.resource_path)
        )
