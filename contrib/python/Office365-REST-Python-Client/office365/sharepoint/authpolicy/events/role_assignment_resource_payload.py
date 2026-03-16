from office365.runtime.client_value import ClientValue


class RoleAssignmentResourcePayload(ClientValue):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.AuthPolicy.Events.RoleAssignmentResourcePayload"
