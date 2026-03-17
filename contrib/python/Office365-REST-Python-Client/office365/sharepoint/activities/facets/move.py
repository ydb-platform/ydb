from office365.runtime.client_value import ClientValue
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath


class MoveFacet(ClientValue):
    """"""

    def __init__(self, from_=SPResPath(), to=SPResPath()):
        self.from_ = from_
        self.to = to

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.MoveFacet"
