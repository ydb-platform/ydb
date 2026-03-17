from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class Office365CommsMessagesServiceProxy(Entity):
    """"""

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.Online.SharePoint.TenantAdministration.Office365CommsMessagesServiceProxy"
        )
        super(Office365CommsMessagesServiceProxy, self).__init__(context, static_path)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.Office365CommsMessagesServiceProxy"
