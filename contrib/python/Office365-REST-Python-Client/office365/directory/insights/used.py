from office365.directory.insights.resource_reference import ResourceReference
from office365.directory.insights.usage_details import UsageDetails
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class UsedInsight(Entity):
    """
    An insight representing documents used by a specific user. The insights returns the most relevant documents
    that a user viewed or modified. This includes documents in:
      OneDrive for Business
      SharePoint
    """

    @property
    def last_used(self):
        """Information about when the item was last viewed or modified by the user."""
        return self.properties.get("lastUsed", UsageDetails())

    @property
    def resource_reference(self):
        """Reference properties of the used document, such as the url and type of the document. Read-only"""
        return self.properties.get("resourceReference", ResourceReference())

    @property
    def resource(self):
        """Used for navigating to the item that was used. For file attachments, the type is fileAttachment.
        For linked attachments, the type is driveItem."""
        return self.properties.get(
            "resource",
            Entity(self.context, ResourcePath("resource", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "lastUsed": self.last_used,
                "resourceReference": self.resource_reference,
            }
            default_value = property_mapping.get(name, None)
        return super(UsedInsight, self).get_property(name, default_value)
