from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.webs.template import WebTemplate


class WebTemplateCollection(EntityCollection[WebTemplate]):
    """Specifies a collection of site templates."""

    def __init__(self, context, resource_path=None, parent=None):
        super(WebTemplateCollection, self).__init__(
            context, WebTemplate, resource_path, parent
        )

    def get_by_name(self, name):
        """Returns the SP.WebTemplate (section 3.2.5.151) specified by its name.
        :param str name: The name of the WebTemplate that is returned.
        """
        return WebTemplate(
            self.context,
            ServiceOperationPath(
                "getByName", ["{name}".format(name=name)], self.resource_path
            ),
        )
