from office365.runtime.client_value import ClientValue


class ViewData(ClientValue):
    def __init__(self, total_hits=None, total_users=None):
        """
        :param int total_hits:
        :param int total_users:
        """
        self.TotalHits = total_hits
        self.TotalUsers = total_users

    @property
    def entity_type_name(self):
        return "SP.Publishing.ViewData"
