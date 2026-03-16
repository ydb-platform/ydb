from office365.runtime.client_value import ClientValue


class SPAddinPermissionInfo(ClientValue):
    """"""

    def __init__(self, absolute_url=None):
        self.absoluteUrl = absolute_url

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Marketplace.CorporateCuratedGallery.SPAddinPermissionInfo"
