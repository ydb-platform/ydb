from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity


class ProtectionPolicyBase(Entity):
    """
    Contains details about protection policies applied to Microsoft 365 data in an organization.
    Protection policies are defined by the Global Admin (or the SharePoint Online Admin or Exchange Online Admin)
    and include what data to protect, when to protect it, and for what time period to retain the protected data
    for a single Microsoft 365 service.
    """

    @property
    def created_by(self):
        """Identity of the user, device, or application which created the item."""
        return self.properties.get("createdBy", IdentitySet())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
            }
            default_value = property_mapping.get(name, None)
        return super(ProtectionPolicyBase, self).get_property(name, default_value)
