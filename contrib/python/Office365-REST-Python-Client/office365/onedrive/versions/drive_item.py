from typing import AnyStr, Optional

from office365.onedrive.versions.base_item import BaseItemVersion
from office365.runtime.queries.service_operation import ServiceOperationQuery


class DriveItemVersion(BaseItemVersion):
    """The DriveItemVersion resource represents a specific version of a DriveItem."""

    def restore_version(self):
        """Restore a previous version of a DriveItem to be the current version.
        This will create a new version with the contents of the previous version, but preserves all existing
        versions of the file."""
        qry = ServiceOperationQuery(self, "restoreVersion")
        self.context.add_query(qry)
        return self

    @property
    def content(self):
        # type: () -> Optional[AnyStr]
        """The content stream for this version of the item."""
        return self.properties.get("content", None)

    @property
    def size(self):
        # type: () -> Optional[int]
        """Indicates the size of the content stream for this version of the item."""
        return self.properties.get("size", None)
