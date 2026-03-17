from office365.directory.object import DirectoryObject
from office365.directory.object_collection import DirectoryObjectCollection
from office365.runtime.paths.resource_path import ResourcePath


class OrgContact(DirectoryObject):
    """Represents an organizational contact. Organizational contacts are managed by an organization's administrators
    and are different from personal contacts. Additionally, organizational contacts are either synchronized
    from on-premises directories or from Exchange Online, and are read-only."""

    @property
    def direct_reports(self):
        """
        Get a user's direct reports.
        """
        return self.properties.get(
            "directReports",
            DirectoryObjectCollection(
                self.context, ResourcePath("directReports", self.resource_path)
            ),
        )

    @property
    def manager(self):
        """
        The user or contact that is this contact's manager.
        """
        return self.properties.get(
            "manager",
            DirectoryObject(self.context, ResourcePath("manager", self.resource_path)),
        )

    @property
    def member_of(self):
        """Groups that this contact is a member of."""
        return self.properties.get(
            "memberOf",
            DirectoryObjectCollection(
                self.context, ResourcePath("memberOf", self.resource_path)
            ),
        )

    @property
    def transitive_member_of(self):
        """Groups that this contact is a member of, including groups that the contact is nested under."""
        return self.properties.get(
            "transitiveMemberOf",
            DirectoryObjectCollection(
                self.context, ResourcePath("transitiveMemberOf", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "directReports": self.direct_reports,
                "memberOf": self.member_of,
                "transitiveMemberOf": self.transitive_member_of,
            }
            default_value = property_mapping.get(name, None)
        return super(OrgContact, self).get_property(name, default_value)
