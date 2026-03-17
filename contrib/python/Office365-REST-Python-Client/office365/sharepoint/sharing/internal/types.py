from office365.runtime.client_value import ClientValue


class CAnonymousLinkUseLimit(ClientValue):

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.SharePoint.Sharing.Internal.CAnonymousLinkUseLimit"


class CExternalSharingEnforcement(ClientValue):

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.SharePoint.Sharing.Internal.CExternalSharingEnforcement"
