from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.marketplace.corporatecuratedgallery.addins.permission_failed_info import (
    SPAddinPermissionFailedInfo,
)
from office365.sharepoint.marketplace.corporatecuratedgallery.addins.permission_info import (
    SPAddinPermissionInfo,
)


class SPAddinPermissionResponse(ClientValue):
    """"""

    def __init__(self, addin_permissions=None, failed_addins=None):
        self.addinPermissions = ClientValueCollection(
            SPAddinPermissionInfo, addin_permissions
        )
        self.failedAddins = ClientValueCollection(
            SPAddinPermissionFailedInfo, failed_addins
        )

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Marketplace.CorporateCuratedGallery.SPAddinPermissionResponse"
