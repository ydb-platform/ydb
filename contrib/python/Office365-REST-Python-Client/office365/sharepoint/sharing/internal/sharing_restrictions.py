from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.internal.types import CAnonymousLinkUseLimit


class SharingRestrictions(Entity):
    """ """

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.SharePoint.Sharing.Internal.SharingRestrictions"
        )
        super(SharingRestrictions, self).__init__(context, static_path)

    @property
    def anonymous_link_use_limit(self):
        return self.properties.get("anonymousLinkUseLimit", CAnonymousLinkUseLimit())

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.SharePoint.Sharing.Internal.SharingRestrictions"
