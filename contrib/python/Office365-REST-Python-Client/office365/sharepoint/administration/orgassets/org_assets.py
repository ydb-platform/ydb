from office365.runtime.client_value import ClientValue
from office365.sharepoint.administration.orgassets.library_collection import (
    OrgAssetsLibraryCollection,
)


class OrgAssets(ClientValue):
    def __init__(self):
        self.OrgAssetsLibraries = OrgAssetsLibraryCollection()

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.OrgAssets"
