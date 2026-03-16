from office365.runtime.client_result import ClientResult
from office365.runtime.client_value import ClientValue
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class GroupInfo(ClientValue):
    """"""

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.GroupInfo"


class SPOGroup(Entity):
    """ """

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.Online.SharePoint.TenantAdministration.SPOGroup"
            )
        super(SPOGroup, self).__init__(context, resource_path)

    def add_as_group_owner_and_member(self, group_id, user_id, user_principal_name):
        """ """
        payload = {
            "groupId": group_id,
            "userId": user_id,
            "userPrincipalName": user_principal_name,
        }
        qry = ServiceOperationQuery(
            self, "AddAsGroupOwnerAndMember", None, payload, None
        )
        self.context.add_query(qry)
        return self

    def get_group_info(self, group_id):
        """"""
        return_type = ClientResult(self.context, GroupInfo())
        payload = {"groupId": group_id}
        qry = ServiceOperationQuery(
            self, "GetGroupInfo", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.SPOGroup"
