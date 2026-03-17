from office365.entity_collection import EntityCollection
from office365.onedrive.listitems.field_value_set import FieldValueSet
from office365.onedrive.versions.base_item import BaseItemVersion
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class ListItemVersion(BaseItemVersion):
    """The listItemVersion resource represents a previous version of a ListItem resource."""

    def restore_version(self):
        """
        Restore a previous version of a DriveItem to be the current version. This will create a new version with
        the contents of the previous version, but preserves all existing versions of the file.
        """
        qry = ServiceOperationQuery(self, "restoreVersion")
        self.context.add_query(qry)
        return self

    @property
    def fields(self):
        # type: () -> EntityCollection[FieldValueSet]
        """A collection of the fields and values for this version of the list item."""
        return self.properties.get(
            "fields",
            EntityCollection(
                self.context, FieldValueSet, ResourcePath("fields", self.resource_path)
            ),
        )
