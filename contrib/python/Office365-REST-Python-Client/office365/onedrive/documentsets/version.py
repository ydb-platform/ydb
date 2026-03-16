import datetime
from typing import Optional

from office365.directory.permissions.identity_set import IdentitySet
from office365.onedrive.documentsets.version_item import DocumentSetVersionItem
from office365.onedrive.versions.list_item import ListItemVersion
from office365.runtime.client_value_collection import ClientValueCollection


class DocumentSetVersion(ListItemVersion):
    """Represents the version of a document set item in a list."""

    @property
    def comment(self):
        # type: () -> Optional[str]
        """Comment about the captured version."""
        return self.properties.get("comment", None)

    @property
    def created_by(self):
        # type: () -> IdentitySet
        """User who captured the version."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def created_datetime(self):
        # type: () -> datetime.datetime
        """Date and time when this version was created."""
        return self.properties("createdDateTime", datetime.datetime.min)

    @property
    def items(self):
        # type: () -> ClientValueCollection[DocumentSetVersionItem]
        """Items within the document set that are captured as part of this version."""
        return self.properties.get(
            "items", ClientValueCollection(DocumentSetVersionItem)
        )

    @property
    def should_capture_minor_version(self):
        # type: () -> Optional[bool]
        """
        If true, minor versions of items are also captured; otherwise, only major versions will be captured.
        Default value is false.
        """
        return self.properties.get("shouldCaptureMinorVersion", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
                "createdDateTime": self.created_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(DocumentSetVersion, self).get_property(name, default_value)
