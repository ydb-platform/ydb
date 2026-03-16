from office365.runtime.client_value import ClientValue


class SearchObjectOwnerResult(ClientValue):
    """This object contains the search object owner in the result."""

    def __init__(self, site_collection_id=None, site_id=None, tenant_id=None):
        """
        :param str site_collection_id: This object contains the site collection id that identifies the owner.
        :param str site_id: This object contains the site id that identifies the owner.
        :param str tenant_id: This object contains the tenant id that identifies the owner
        """
        self.SiteCollectionId = site_collection_id
        self.SiteId = site_id
        self.TenantId = tenant_id

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SearchObjectOwnerResult"
