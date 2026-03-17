from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.tenant.administration.site_user_group_info import (
    SiteUserGroupInfo,
)
from office365.sharepoint.tenant.administration.sites.properties import SiteProperties
from office365.sharepoint.tenant.administration.sites.state_properties import (
    SiteStateProperties,
)


class SitePropertiesCollection(EntityCollection[SiteProperties]):
    """SiteProperties resource collection"""

    def __init__(self, context, resource_path=None):
        super(SitePropertiesCollection, self).__init__(
            context, SiteProperties, resource_path
        )

    def get_by_id(self, site_id):
        """
        :param str site_id: Site identifier
        """
        return_type = SiteProperties(self.context)
        qry = ServiceOperationQuery(self, "GetById", [site_id], None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_lock_state_by_id(self, site_id):
        """
        :param str site_id: Site identifier
        """
        return_type = ClientResult(self.context, int())
        qry = ServiceOperationQuery(
            self, "GetLockStateById", [site_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_state_properties(self, site_id):
        """
        Gets site state properties.

        :param str site_id: Site identifier
        """
        return_type = ClientResult(self.context, SiteStateProperties())
        qry = ServiceOperationQuery(
            self, "GetSiteStateProperties", [site_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_user_groups(self, site_id):
        """
        Gets site user groups.

        :param str site_id: Site identifier
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(SiteUserGroupInfo)
        )
        qry = ServiceOperationQuery(
            self, "GetSiteUserGroups", [site_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def check_site_is_archived_by_id(self, site_id):
        """
        :param str site_id: Site identifier
        """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "CheckSiteIsArchivedById", [site_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
