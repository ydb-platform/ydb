from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.alerts.collection import AlertCollection
from office365.sharepoint.principal.principal import Principal
from office365.sharepoint.principal.users.id_info import UserIdInfo
from office365.sharepoint.userprofiles.person_properties import PersonProperties


class User(Principal):
    """Represents a user in Microsoft SharePoint Foundation. A user is a type of SP.Principal."""

    def get_personal_site(self):
        """Get personal site for a user"""
        from office365.sharepoint.sites.site import Site

        return_type = Site(self.context)

        def _person_props_loaded(person_props):
            # type: (PersonProperties) -> None
            return_type.set_property("__siteUrl", person_props.personal_url)

        def _get_properties_for():
            self.context.people_manager.get_properties_for(
                self.login_name
            ).after_execute(_person_props_loaded)

        self.ensure_property("LoginName", _get_properties_for)
        return return_type

    def get_recent_files(self, top=100):
        """"""
        from office365.sharepoint.files.recent_file_collection import (
            RecentFileCollection,
        )

        return_type = RecentFileCollection.get_recent_files(self.context, top)
        return return_type

    def get_user_profile_properties(self, property_names=None):
        """
        :param list[str] property_names:
        """
        from office365.sharepoint.userprofiles.properties_for_user import (
            UserProfilePropertiesForUser,
        )

        return_type = UserProfilePropertiesForUser(self.context)

        def _user_loaded():
            return_type.set_property("PropertyNames", property_names)
            return_type.set_property("AccountName", self.user_principal_name)

        self.ensure_property("UserPrincipalName", _user_loaded)
        return return_type

    def expire(self):
        """"""
        qry = ServiceOperationQuery(self, "Expire")
        self.context.add_query(qry)
        return self

    @property
    def aad_object_id(self):
        """Gets the information of the user that contains the user's name identifier and the issuer of the
        user's name identifier."""
        return self.properties.get("AadObjectId", UserIdInfo())

    @property
    def alerts(self):
        """Gets site alerts for this user."""
        return self.properties.get(
            "Alerts",
            AlertCollection(self.context, ResourcePath("Alerts", self.resource_path)),
        )

    @property
    def groups(self):
        """Gets a collection of group objects that represents all of the groups for the user."""
        from office365.sharepoint.principal.groups.collection import GroupCollection

        return self.properties.get(
            "Groups",
            GroupCollection(self.context, ResourcePath("Groups", self.resource_path)),
        )

    @property
    def is_site_admin(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether the user is a site collection administrator."""
        return self.properties.get("IsSiteAdmin", None)

    @property
    def user_id(self):
        """Gets the information of the user that contains the user's name identifier and the issuer of the
        user's name identifier."""
        return self.properties.get("UserId", UserIdInfo())

    @property
    def email(self):
        # type: () -> Optional[str]
        """
        Specifies the e-mail address of the user.
        It MUST NOT be NULL. Its length MUST be equal to or less than 255.
        """
        return self.properties.get("Email", None)

    @property
    def email_with_fallback(self):
        # type: () -> Optional[str]
        return self.properties.get("EmailWithFallback", None)

    @property
    def expiration(self):
        # type: () -> Optional[str]
        return self.properties.get("Expiration", None)

    @property
    def is_email_authentication_guest_user(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the User is a share by email guest user using time of access authentication.
        If this instance is an email authentication guest user, this value MUST be true, otherwise it MUST be false.
        """
        return self.properties.get("IsEmailAuthenticationGuestUser", None)

    @property
    def is_share_by_email_guest_user(self):
        # type: () -> Optional[bool]
        """
        Gets a value indicating whether this User is a share by email guest user.
        If this instance is a share by email guest user, it's true; otherwise, false.
        """
        return self.properties.get("IsShareByEmailGuestUser", None)

    @property
    def user_principal_name(self):
        # type: () -> Optional[str]
        """User principal name of the user that initiated the sign-in."""
        return self.properties.get("UserPrincipalName", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "AadObjectId": self.aad_object_id,
                "UserId": self.user_id,
            }
            default_value = property_mapping.get(name, None)
        return super(User, self).get_property(name, default_value)
