from office365.runtime.client_value import ClientValue
from office365.sharepoint.activities.facets.revision_set import RevisionSetFacet


class ActivityClientRequest(ClientValue):
    def __init__(self, revisionSet=RevisionSetFacet()):
        self.revisionSet = revisionSet

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.ActivityClientRequest"
