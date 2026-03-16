from office365.runtime.client_value import ClientValue


class SiteStateProperties(ClientValue):
    def __init__(
        self,
        GroupSiteRelationship=None,
        IsArchived=None,
        IsSiteOnHold=None,
        LockState=None,
    ):
        # type: (int, bool, bool, int) -> None
        super(SiteStateProperties, self).__init__()
        self.GroupSiteRelationship = GroupSiteRelationship
        self.IsArchived = IsArchived
        self.IsSiteOnHold = IsSiteOnHold
        self.LockState = LockState

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SiteStateProperties"
