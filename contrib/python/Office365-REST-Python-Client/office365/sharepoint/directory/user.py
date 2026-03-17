from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.sharepoint.directory.my_groups_result import MyGroupsResult
from office365.sharepoint.entity import Entity


class User(Entity):
    """Represents a user in the SharePoint Directory"""

    @property
    def about_me(self):
        # type: () -> Optional[str]
        """Stores a short description or bio of the user."""
        return self.properties.get("aboutMe", None)

    @property
    def account_enabled(self):
        # type: () -> Optional[bool]
        """Gets weather the account is active (user can log in and access services) or not"""
        return self.properties.get("accountEnabled", None)

    def is_member_of(self, group_id):
        # type: (str) -> ClientResult[bool]
        return_type = ClientResult(self.context)

        def _user_loaded():
            from office365.sharepoint.directory.helper import SPHelper

            SPHelper.is_member_of(
                self.context, self.properties["principalName"], group_id, return_type
            )

        self.ensure_property("principalName", _user_loaded)
        return return_type

    def get_my_groups(self):
        """
        Retrieves information about groups that a user belongs to.
        """
        return_type = MyGroupsResult(self.context)

        def _user_loaded():
            from office365.sharepoint.directory.helper import SPHelper

            SPHelper.get_my_groups(
                self.context, self.properties["principalName"], 0, 10, return_type
            )

        self.ensure_property("principalName", _user_loaded)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Directory.User"
