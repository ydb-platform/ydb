from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.principal.users.user import User
from office365.sharepoint.userprofiles.hash_tag import HashTagCollection
from office365.sharepoint.userprofiles.person_properties import PersonProperties
from office365.sharepoint.userprofiles.personal_site_creation_priority import (
    PersonalSiteCreationPriority,
)


def _ensure_user(user_or_name, action):
    """
    :param str or User user_or_name: User or Login name of the specified user.
    :param (str) -> None action: Callback
    """
    if isinstance(user_or_name, User):

        def _user_loaded():
            action(user_or_name.login_name)

        user_or_name.ensure_property("LoginName", _user_loaded)
    else:
        action(user_or_name)


class PeopleManager(Entity):
    """Provides methods for operations related to people."""

    def __init__(self, context):
        super(PeopleManager, self).__init__(
            context, ResourcePath("SP.UserProfiles.PeopleManager")
        )

    @staticmethod
    def get_trending_tags(context):
        """Gets a collection of the 20 (or fewer) most popular hash tags over the past week.
        The returned collection is sorted in descending order of frequency of use.

        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = HashTagCollection(context)
        manager = PeopleManager(context)
        qry = ServiceOperationQuery(
            manager, "GetTrendingTags", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    def get_user_onedrive_quota_max(self, account_name):
        """
        :param str account_name: Account name of the specified user.
        """
        return_type = ClientResult(self.context, int())
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(
            self, "GetUserOneDriveQuotaMax", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def am_i_following(self, account_name):
        """
        Checks whether the current user is following the specified user.

        :param str account_name: Account name of the specified user.
        :return:
        """
        result = ClientResult(self.context)
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(self, "AmIFollowing", params, None, None, result)
        self.context.add_query(qry)
        return result

    def get_followers_for(self, account):
        # type: (str|User) -> EntityCollection[PersonProperties]
        """
        Gets the people who are following the specified user.

        :param str|User account: Account name of the specified user.
        """
        return_type = EntityCollection(self.context, PersonProperties)

        def _get_followers_for(account_name):
            params = {"accountName": account_name}
            qry = ServiceOperationQuery(
                self, "GetFollowersFor", params, None, None, return_type
            )
            self.context.add_query(qry)

        if isinstance(account, User):

            def _account_loaded():
                _get_followers_for(account.login_name)

            account.ensure_property("LoginName", _account_loaded)
        else:
            _get_followers_for(account)
        return return_type

    def get_user_information(self, account_name, site_id):
        """
        :param str account_name: Account name of the specified user.
        :param str site_id: Site Identifier.
        """
        return_type = ClientResult(self.context, {})
        params = {"accountName": account_name, "siteId": site_id}
        qry = ServiceOperationQuery(
            self, "GetSPUserInformation", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def follow(self, account_name):
        """
        Add the specified user to the current user's list of followed users.

        :param str account_name: Account name of the specified user.
        """
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(self, "Follow", params, None, None, None)
        self.context.add_query(qry)
        return self

    def stop_following(self, account_name):
        """
        Remove the specified user from the current user's list of followed users.

        :param str account_name:
        """
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(self, "StopFollowing", params)
        self.context.add_query(qry)
        return self

    def stop_following_tag(self, value):
        """
        The StopFollowingTag method sets the current user to no longer be following the specified tag.

        :param str value: Specifies the tag by its GUID
        """
        params = {"value": value}
        qry = ServiceOperationQuery(self, "StopFollowingTag", params)
        self.context.add_query(qry)
        return self

    def get_user_profile_properties(self, user_or_name):
        # type: (str|User) -> ClientResult[dict]
        """
        Gets the specified user profile properties for the specified user.

        :param str or User user_or_name: User or Login name of the specified user.
        """
        return_type = ClientResult(self.context, {})

        def _user_resolved(account_name):
            # type: (str) -> None
            params = {"accountName": account_name}
            qry = ServiceOperationQuery(
                self, "GetUserProfileProperties", params, None, None, return_type
            )
            self.context.add_query(qry)

        _ensure_user(user_or_name, _user_resolved)
        return return_type

    def get_properties_for(self, account):
        """
        Gets user properties for the specified user.
        :param str or User account: Specifies the User object or its login name.
        """
        return_type = PersonProperties(self.context)

        def _get_properties_for_inner(account_name):
            # type: (str) -> None
            params = {"accountName": account_name}
            qry = ServiceOperationQuery(
                self, "GetPropertiesFor", params, None, None, return_type
            )
            self.context.add_query(qry)

        _ensure_user(account, _get_properties_for_inner)
        return return_type

    def get_default_document_library(
        self,
        user_or_name,
        create_site_if_not_exists=False,
        site_creation_priority=PersonalSiteCreationPriority.Low,
    ):
        """
        Gets the OneDrive Document library path for a given user.

        :param str or User user_or_name user_or_name: The login name of the user whose OneDrive URL is required.
             For example, "i:0#.f|membership|admin@contoso.sharepoint.com”.
        :param bool create_site_if_not_exists: If this value is set to true and the site doesn't exist, the site will
            get created.
        :param int site_creation_priority: The priority for site creation. Type: PersonalSiteCreationPriority
        """
        return_type = ClientResult(self.context)

        def _get_default_document_library(account_name):
            params = {
                "accountName": account_name,
                "createSiteIfNotExists": create_site_if_not_exists,
                "siteCreationPriority": site_creation_priority,
            }
            qry = ServiceOperationQuery(
                self, "GetDefaultDocumentLibrary", params, None, None, return_type
            )
            self.context.add_query(qry)

        _ensure_user(user_or_name, _get_default_document_library)
        return return_type

    def get_people_followed_by(self, account_name):
        """
        The GetPeopleFollowedBy method returns a  list of PersonProperties objects for people who the specified user
        is following. This method can result in exceptions for conditions such as null arguments or if the specified
        user cannot be found.

        :param str account_name: Account name of the specified user.
        """
        return_type = EntityCollection(self.context, PersonProperties)
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(
            self, "GetPeopleFollowedBy", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_my_followers(self):
        """
        This method returns a list of PersonProperties objects for the people who are following the current user.
        """
        return_type = EntityCollection(self.context, PersonProperties)
        qry = ServiceOperationQuery(
            self, "GetMyFollowers", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def follow_tag(self, value):
        """
        The FollowTag method sets the current user to be following the specified tag.
        :param str value: Specifies the tag by its GUID.
        """

        qry = ServiceOperationQuery(self, "FollowTag", [value])
        self.context.add_query(qry)
        return self

    def hide_suggestion(self, account_name):
        """The HideSuggestion method adds the specified user to list of rejected suggestions.

        :param str account_name: Specifies the user by account name.
        """
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(self, "HideSuggestion", params)
        self.context.add_query(qry)
        return self

    def reset_user_onedrive_quota_to_default(self, account_name):
        """
        :param str account_name: Specifies the user by account name.
        """
        return_type = ClientResult(self.context, str())
        params = {"accountName": account_name}
        qry = ServiceOperationQuery(
            self, "ResetUserOneDriveQuotaToDefault", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_my_profile_picture(self, picture):
        """
        The SetMyProfilePicture method uploads and sets the user profile picture. Pictures in bmp, jpg and png formats
        and up to 5,000,000 bytes are supported. A user can upload a picture only to the user's own profile.

        :param str or bytes picture: Binary content of an image file
        """
        qry = ServiceOperationQuery(
            self, "SetMyProfilePicture", None, {"picture": picture}
        )
        self.context.add_query(qry)
        return self

    def set_user_onedrive_quota(self, account_name, new_quota, new_quota_warning):
        """
        :param str account_name:
        :param long new_quota:
        :param long new_quota_warning:
        """
        return_type = ClientResult(self.context, str())
        payload = {
            "accountName": account_name,
            "newQuota": new_quota,
            "newQuotaWarning": new_quota_warning,
        }
        qry = ServiceOperationQuery(
            self, "SetUserOneDriveQuota", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_multi_valued_profile_property(
        self, account_name, property_name, property_values
    ):
        """
        Sets the value of a multivalued user profile property.
        :param str account_name: Specifies the user by account name.
        :param str property_name: The name of the property to set.
        :param list[str] property_values: The values being set on the property.
        """
        payload = {
            "accountName": account_name,
            "propertyName": property_name,
            "propertyValues": StringCollection(property_values),
        }
        qry = ServiceOperationQuery(
            self, "SetMultiValuedProfileProperty", None, payload
        )
        self.context.add_query(qry)
        return self

    def set_single_value_profile_property(
        self, account_name, property_name, property_value
    ):
        """
        Sets the value of a user profile property.

        :param str account_name: Specifies the user by account name.
        :param str property_name: The name of the property to set.
        :param str property_value: The value being set on the property.
        """
        payload = {
            "accountName": account_name,
            "propertyName": property_name,
            "propertyValue": property_value,
        }
        qry = ServiceOperationQuery(
            self, "SetSingleValueProfileProperty", None, payload
        )
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.PeopleManager"
