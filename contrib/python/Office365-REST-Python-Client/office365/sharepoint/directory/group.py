from office365.sharepoint.directory.helper import SPHelper
from office365.sharepoint.directory.members_info import MembersInfo
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection


class Group(Entity):
    """ """

    def get_members_info(self, row_limit):
        """"""
        return_type = MembersInfo(self.context)

        def _user_loaded():
            from office365.sharepoint.directory.helper import SPHelper

            SPHelper.get_members_info(
                self.context, self.properties["Id"], row_limit, return_type
            )

        self.ensure_property("Id", _user_loaded)
        return return_type

    def get_members(self):
        """"""
        from office365.directory.users.user import User

        return_type = EntityCollection(self.context, User)

        def _group_loaded():
            SPHelper.get_members(self.context, self.properties["Id"], return_type)

        self.ensure_property("Id", _group_loaded)
        return return_type

    def get_owners(self):
        """"""
        from office365.directory.users.user import User

        return_type = EntityCollection(self.context, User)

        def _group_loaded():
            SPHelper.get_owners(self.context, self.properties["Id"], return_type)

        self.ensure_property("Id", _group_loaded)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Directory.Group"
