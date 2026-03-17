from typing import TYPE_CHECKING, Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.principal.principal import Principal
from office365.sharepoint.utilities.principal_info import PrincipalInfo

if TYPE_CHECKING:
    from office365.sharepoint.principal.groups.collection import GroupCollection


class Group(Principal):
    """Represents a collection of members in a SharePoint site. A group is a type of SP.Principal."""

    def delete_object(self):
        """
        Deletes the group
        A custom operation since the default type SP.Group does not support HTTP DELETE method.
        """
        if self.id:
            self.parent_collection.remove_by_id(self.id)
        elif self.login_name:
            self.parent_collection.remove_by_login_name(self.login_name)
        else:

            def _group_loaded():
                self.parent_collection.remove_by_id(self.id)

            self.ensure_property("Id", _group_loaded)
        return self

    def expand_to_principals(self, max_count=10):
        """
        Expands current group to a collection of principals.
        :param int max_count: Specifies the maximum number of principals to be returned.
        """
        return_type = ClientResult(self.context, ClientValueCollection(PrincipalInfo))

        def _group_loaded():
            from office365.sharepoint.utilities.utility import Utility

            Utility.expand_groups_to_principals(
                self.context, [self.login_name], max_count, return_type
            )

        self.ensure_property("LoginName", _group_loaded)
        return return_type

    def set_user_as_owner(self, user):
        """
        Sets the user as group owner
        :param long or Principal user: User object or identifier
        """

        def _set_user_as_owner(owner_id):
            qry = ServiceOperationQuery(
                self, "SetUserAsOwner", None, {"ownerId": owner_id}
            )
            self.context.add_query(qry)

        if isinstance(user, Principal):

            def _user_loaded():
                _set_user_as_owner(user.id)

            user.ensure_property("Id", _user_loaded)
        else:
            _set_user_as_owner(user)
        return self

    @property
    def parent_collection(self):
        # type: () -> GroupCollection
        return self._parent_collection

    @property
    def allow_members_edit_membership(self):
        # type: () -> Optional[bool]
        """Specifies whether a member of the group can add and remove members from the group"""
        return self.properties.get("AllowMembersEditMembership", None)

    @property
    def allow_request_to_join_leave(self):
        # type: () -> Optional[bool]
        """Specifies whether to allow users to request to join or leave in the group."""
        return self.properties.get("AllowRequestToJoinLeave", None)

    @property
    def auto_accept_request_to_join_leave(self):
        # type: () -> Optional[bool]
        """Specifies whether requests to join or leave the group are automatically accepted"""
        return self.properties.get("AutoAcceptRequestToJoinLeave", None)

    @property
    def can_current_user_edit_membership(self):
        # type: () -> Optional[bool]
        """Specifies whether the current user can add and remove members from the group."""
        return self.properties.get("CanCurrentUserEditMembership", None)

    @property
    def can_current_user_manage_group(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that indicates whether the current user can manage the group."""
        return self.properties.get("CanCurrentUserManageGroup", None)

    @property
    def can_current_user_view_membership(self):
        # type: () -> Optional[bool]
        """Specifies whether the current user can view the membership of the group."""
        return self.properties.get("CanCurrentUserViewMembership", None)

    @property
    def only_allow_members_view_membership(self):
        # type: () -> Optional[bool]
        """Specifies whether viewing the membership of the group is restricted to members of the group."""
        return self.properties.get("OnlyAllowMembersViewMembership", None)

    @property
    def owner_title(self):
        # type: () -> Optional[str]
        """Specifies the name of the owner of the group."""
        return self.properties.get("OwnerTitle", None)

    @property
    def request_to_join_leave_email_setting(self):
        # type: () -> Optional[str]
        """Specifies the e-mail address to which requests to join or leave the group are sent."""
        return self.properties.get("RequestToJoinLeaveEmailSetting", None)

    @property
    def owner(self):
        """Specifies the owner of the group."""
        return self.properties.get(
            "Owner", Principal(self.context, ResourcePath("Owner", self.resource_path))
        )

    @property
    def users(self):
        """Gets a collection of user objects that represents all of the members in the group."""
        from office365.sharepoint.principal.users.collection import UserCollection

        return self.properties.get(
            "Users",
            UserCollection(self.context, ResourcePath("Users", self.resource_path)),
        )
