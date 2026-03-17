from typing import TYPE_CHECKING, Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.fontpackages.font_package import FontPackage

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class FontPackageCollection(EntityCollection):
    """Represents a collection of View resources."""

    def __init__(self, context, resource_path=None):
        # type: (ClientContext, Optional[ResourcePath]) -> None
        super(FontPackageCollection, self).__init__(context, FontPackage, resource_path)

    def get_by_title(self, title):
        """
        :param str title: The title of the font package to return.
        """
        return FontPackage(
            self.context,
            ServiceOperationPath("GetByTitle", [title], self.resource_path),
        )
