from office365.runtime.client_value import ClientValue


class SiteMoveServiceInfo(ClientValue):
    """ """

    @property
    def entity_type_name(self):
        return (
            "Microsoft.SharePoint.Administration.SiteMove.Service.SiteMoveServiceInfo"
        )
