from office365.runtime.client_value import ClientValue


class RenameFacet(ClientValue):
    def __init__(self, old_name=None):
        """
        :param str old_name:
        """
        self.oldName = old_name

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.RenameFacet"
