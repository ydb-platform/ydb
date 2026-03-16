from office365.runtime.client_object import ClientObject
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.userprofiles.user_profile import UserProfile


class ProfileLoader(ClientObject):
    """The ProfileLoader class provides access to the current user's profile."""

    def __init__(self, context):
        super(ProfileLoader, self).__init__(
            context, ResourcePath("SP.UserProfiles.ProfileLoader.GetProfileLoader")
        )

    @staticmethod
    def get_profile_loader(context):
        """
        The GetProfileLoader method returns a profile loader.

        :type: office365.sharepoint.client_context.ClientContext context
        """
        return_type = ProfileLoader(context)
        qry = ServiceOperationQuery(
            return_type, "GetProfileLoader", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_owner_user_profile(context):
        """
        Gets the user profile for the Site owner.

        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = UserProfile(context)
        qry = ServiceOperationQuery(
            ProfileLoader(context),
            "GetOwnerUserProfile",
            None,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    def get_user_profile(self):
        """The GetUserProfile method returns the user profile for the current user."""
        result = UserProfile(
            self.context, ResourcePath("GetUserProfile", self.resource_path)
        )
        qry = ServiceOperationQuery(self, "GetUserProfile", None, None, None, result)
        self.context.add_query(qry)
        return result

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.ProfileLoader"
