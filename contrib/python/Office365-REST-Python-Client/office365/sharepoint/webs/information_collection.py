from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.webs.information import WebInformation


class WebInformationCollection(EntityCollection[WebInformation]):
    """Specifies a collection of objects containing metadata about a site"""

    def __init__(self, context, resource_path=None):
        super(WebInformationCollection, self).__init__(
            context, WebInformation, resource_path
        )

    def get_by_id(self, _id):
        """Returns an SP.WebInformation (section 3.2.5.192) object that contains metadata about a site (2) specified
        by the identifier of the site

        :param str _id: Specifies the identifier of site
        """
        return WebInformation(
            self.context, ServiceOperationPath("GetById", [_id], self.resource_path)
        )
