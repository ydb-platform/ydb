from office365.runtime.client_value import ClientValue


class RefinerEntry(ClientValue):
    def __init__(self, refinement_count=None, refinement_name=None):
        """
        :param int refinement_count:
        :param str refinement_name:
        """
        self.RefinementCount = refinement_count
        self.RefinementName = refinement_name

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.RefinerEntry"
