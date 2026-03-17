from typing import Optional

from office365.runtime.odata.type import ODataType
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity


class PersonProperties(Entity):
    """
    The PersonProperties class contains the data about people and is returned by PeopleManager methods
    (see section 3.1.5.58).
    """

    @property
    def account_name(self):
        # type: () -> Optional[str]
        """The AccountName property specifies the person's account name."""
        return self.properties.get("AccountName", None)

    @property
    def email(self):
        # type: () -> Optional[str]
        """The Email property specifies the person's email address."""
        return self.properties.get("Email", None)

    @property
    def latest_post(self):
        # type: () -> Optional[str]
        """The  LatestPost property specifies the person's latest microblog post."""
        return self.properties.get("LatestPost", None)

    @property
    def peers(self):
        """
        The Peers property specifies an array of strings that specify the account names of person's peers, that is,
        those who have the same manager.
        """
        return self.properties.get("Peers", StringCollection())

    @property
    def personal_url(self):
        # type: () -> Optional[str]
        """The PersonalUrl property specifies the absolute URL of the person's personal page."""
        return self.properties.get("PersonalUrl", None)

    @property
    def extended_managers(self):
        """
        The ExtendedManagers property specifies an array of strings that specify the account names of
        a person's managers.
        """
        return self.properties.get("ExtendedManagers", StringCollection())

    @property
    def extended_reports(self):
        """
        The ExtendedReports properties specifies an array of strings that specify the account names of
        person's extended reports.
        """
        return self.properties.get("ExtendedReports", StringCollection())

    @property
    def picture_url(self):
        # type: () -> Optional[str]
        """The PictureUrl property specifies the  URL for the person's profile picture."""
        return self.properties.get("PictureUrl", None)

    @property
    def user_url(self):
        # type: () -> Optional[str]
        """The UserUrl property specifies the URL for the person's profile."""
        return self.properties.get("UserUrl", None)

    @property
    def user_profile_properties(self):
        """"""
        return self.properties.get("UserProfileProperties", None)

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.PersonProperties"

    def set_property(self, k, v, persist_changes=True):
        if k == "UserProfileProperties":
            v = ODataType.parse_key_value_collection(v)
        super(PersonProperties, self).set_property(k, v)
        return self

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "ExtendedManagers": self.extended_managers,
                "ExtendedReports": self.extended_reports,
            }
            default_value = property_mapping.get(name, None)
        return super(PersonProperties, self).get_property(name, default_value)
