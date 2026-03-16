from office365.runtime.client_value import ClientValue


class RecentAndJoinedTeamsResponse(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.RecentAndJoinedTeamsResponse"
