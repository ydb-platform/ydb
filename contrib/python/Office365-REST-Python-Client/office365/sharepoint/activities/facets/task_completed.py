from office365.runtime.client_value import ClientValue


class TaskCompletedFacet(ClientValue):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.TaskCompletedFacet"
