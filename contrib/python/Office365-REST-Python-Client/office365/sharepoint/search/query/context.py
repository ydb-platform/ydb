from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class QueryContext(ClientValue):
    """This object contains the query context properties."""

    def __init__(self, group_object_ids=None, site_id=None, tenant_instance_id=None):
        """
        :param str site_id: This property contains the site identification.
        """
        self.GroupObjectIds = StringCollection(group_object_ids)
        self.SpSiteId = site_id
        self.TenantInstanceId = tenant_instance_id

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.QueryContext"
