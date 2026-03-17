from office365.directory.insights.resource_reference import ResourceReference
from office365.directory.insights.sharing_detail import SharingDetail
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath


class SharedInsight(Entity):
    """
    An insight representing files shared with or by a specific user. The following shared files are supported:

      Files attached directly in an email or a meeting invite.
      OneDrive for Business and SharePoint modern attachments - files stored in OneDrive for Business and SharePoint
      that users share as a links in an email.
    """

    @property
    def last_shared(self):
        """Details about the shared item. Read-only"""
        return self.properties.get("lastShared", SharingDetail())

    @property
    def resource_reference(self):
        # type: () -> ResourceReference
        """Reference properties of the used document, such as the url and type of the document. Read-only"""
        return self.properties.get("resourceReference", ResourceReference())

    @property
    def resource(self):
        # type: () -> Entity
        """Used for navigating to the item that was shared. For file attachments, the type is fileAttachment.
        For linked attachments, the type is driveItem."""
        return self.properties.get(
            "resource",
            Entity(self.context, ResourcePath("resource", self.resource_path)),
        )

    @property
    def sharing_history(self):
        # type: () -> ClientValueCollection[SharingDetail]
        """Details about the sharing history. Read-only"""
        return self.properties.get(
            "sharingHistory", ClientValueCollection(SharingDetail)
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "lastShared": self.last_shared,
                "resourceReference": self.resource_reference,
                "sharingHistory": self.sharing_history,
            }
            default_value = property_mapping.get(name, None)
        return super(SharedInsight, self).get_property(name, default_value)
