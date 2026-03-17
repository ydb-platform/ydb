from office365.runtime.client_value import ClientValue


class InDocFacet(ClientValue):
    """"""

    def __init__(self, contentId=None, navigationId=None):
        self.contentId = contentId
        self.navigationId = navigationId

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.InDocFacet"
