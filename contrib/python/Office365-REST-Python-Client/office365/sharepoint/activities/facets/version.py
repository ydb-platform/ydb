from office365.runtime.client_value import ClientValue


class VersionFacet(ClientValue):
    """"""

    def __init__(self, fromVersion=None):
        self.fromVersion = fromVersion

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.VersionFacet"
