from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class ConsumerPermissions(Entity):
    def __init__(self, context, path=None):
        if path is None:
            path = ResourcePath("Microsoft.SharePoint.Convergence.ConsumerPermissions")
        super(ConsumerPermissions, self).__init__(context, path)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Convergence.ConsumerPermissions"
