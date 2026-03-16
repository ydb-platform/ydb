from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.directory.members_info import MembersInfo
from office365.sharepoint.directory.membership_result import MembershipResult
from office365.sharepoint.directory.my_groups_result import MyGroupsResult
from office365.sharepoint.directory.user import User
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection


class SPHelper(Entity):
    def __init__(self, context):
        super(SPHelper, self).__init__(context, ResourcePath("SP.Directory.SPHelper"))

    @staticmethod
    def is_member_of(context, principal_name, group_id, result=None):
        # type: (ClientContext, str, str, Optional[ClientResult[bool]]) -> ClientResult[bool]
        """
        :param str principal_name: User principal name
        :param str group_id: Group id
        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param ClientResult or None result: Client result
        """
        if result is None:
            result = ClientResult(context)
        payload = {"principalName": principal_name, "groupId": group_id}
        qry = ServiceOperationQuery(
            SPHelper(context), "IsMemberOf", None, payload, None, result, True
        )
        context.add_query(qry)
        return result

    @staticmethod
    def check_site_availability(context, site_url):
        # type: (ClientContext, str) -> ClientResult[bool]
        """ """
        return_type = ClientResult(context)
        qry = ServiceOperationQuery(
            SPHelper(context),
            "CheckSiteAvailability",
            None,
            {"siteUrl": site_url},
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_membership(context, user_id):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str user_id: User's identifier
        """
        payload = {"userId": user_id}
        return_type = MembershipResult(context)
        qry = ServiceOperationQuery(
            SPHelper(context), "GetMembership", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_members_info(context, group_id, row_limit, return_type=None):
        """
        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str group_id: User's login
        :param int row_limit: Result offset
        :param MembersInfo return_type: Result
        """
        if return_type is None:
            return_type = MembersInfo(context)
        payload = {
            "groupId": group_id,
            "rowLimit": row_limit,
        }
        qry = ServiceOperationQuery(
            SPHelper(context), "GetMembersInfo", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_my_groups(context, logon_name, offset, length, return_type=None):
        """
        Retrieves information about groups that a user belongs to.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str logon_name: User's login
        :param int offset: Result offset
        :param int length: Results count
        :param MyGroupsResult return_type: return type
        """
        if return_type is None:
            return_type = MyGroupsResult(context)
        payload = {"logOnName": logon_name, "offset": offset, "len": length}
        qry = ServiceOperationQuery(
            SPHelper(context), "GetMyGroups", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_members(context, group_id, return_type=None):
        """
        :param str group_id: Group identifier
        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param EntityCollection or None return_type: Returns members
        """
        if return_type is None:
            return_type = EntityCollection(context, User)
        qry = ServiceOperationQuery(
            SPHelper(context), "GetMembers", [group_id], None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_owners(context, group_id, return_type=None):
        # type: (ClientContext, str, Optional[EntityCollection[User]]) -> SPHelper
        """
        :param str group_id: Group identifier
        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param EntityCollection or None return_type: Returns members
        """
        if return_type is None:
            return_type = EntityCollection(context, User)
        qry = ServiceOperationQuery(
            SPHelper(context), "GetOwners", [group_id], None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def remove_external_members(context, group_id):
        # type: (ClientContext, str) -> SPHelper
        """
        :param str group_id: Group identifier
        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        """
        binding_type = SPHelper(context)
        qry = ServiceOperationQuery(
            binding_type, "RemoveExternalMembers", [group_id], is_static=True
        )
        context.add_query(qry)
        return binding_type

    @property
    def entity_type_name(self):
        return "SP.Directory.SPHelper"
