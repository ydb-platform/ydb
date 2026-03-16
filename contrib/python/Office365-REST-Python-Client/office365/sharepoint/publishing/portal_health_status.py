from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.publishing.portal_health_details import PortalHealthDetails


class PortalHealthStatus(ClientValue):
    def __init__(self, details=None, status=None):
        """
        :param int status:
        """
        self.Details = ClientValueCollection(PortalHealthDetails, details)
        self.Status = status

    @property
    def entity_type_name(self):
        return "SP.Publishing.PortalHealthStatus"
