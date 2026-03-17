from office365.entity_collection import EntityCollection
from office365.onedrive.analytics.item_analytics import ItemAnalytics
from office365.onedrive.base_item import BaseItem
from office365.onedrive.contenttypes.info import ContentTypeInfo
from office365.onedrive.documentsets.version import DocumentSetVersion
from office365.onedrive.internal.queries.get_activities_by_interval import (
    build_get_activities_by_interval_query,
)
from office365.onedrive.listitems.field_value_set import FieldValueSet
from office365.onedrive.versions.list_item import ListItemVersion
from office365.runtime.paths.resource_path import ResourcePath


class ListItem(BaseItem):
    """Represents an item in a SharePoint list. Column values in the list are available through the fieldValueSet
    dictionary."""

    def get_activities_by_interval(self, start_dt=None, end_dt=None, interval=None):
        """
        Get a collection of itemActivityStats resources for the activities that took place on this resource
        within the specified time interval.

        :param datetime.datetime start_dt: The start time over which to aggregate activities.
        :param datetime.datetime end_dt: The end time over which to aggregate activities.
        :param str interval: The aggregation interval.
        """
        qry = build_get_activities_by_interval_query(self, start_dt, end_dt, interval)
        self.context.add_query(qry)
        return qry.return_type

    @property
    def fields(self):
        """The values of the columns set on this list item."""
        return self.properties.get(
            "fields",
            FieldValueSet(self.context, ResourcePath("fields", self.resource_path)),
        )

    @property
    def versions(self):
        # type: () -> EntityCollection[ListItemVersion]
        """The list of previous versions of the list item."""
        return self.properties.get(
            "versions",
            EntityCollection(
                self.context,
                ListItemVersion,
                ResourcePath("versions", self.resource_path),
            ),
        )

    @property
    def drive_item(self):
        """For document libraries, the driveItem relationship exposes the listItem as a driveItem."""
        from office365.onedrive.driveitems.driveItem import DriveItem

        return self.properties.get(
            "driveItem",
            DriveItem(self.context, ResourcePath("driveItem", self.resource_path)),
        )

    @property
    def content_type(self):
        # type: () -> ContentTypeInfo
        """The content type of this list item"""
        return self.properties.get("contentType", ContentTypeInfo())

    @property
    def analytics(self):
        """Analytics about the view activities that took place on this item."""
        return self.properties.get(
            "analytics",
            ItemAnalytics(self.context, ResourcePath("analytics", self.resource_path)),
        )

    @property
    def document_set_versions(self):
        # type: () -> EntityCollection[DocumentSetVersion]
        """Version information for a document set version created by a user."""
        return self.properties.get(
            "documentSetVersions",
            EntityCollection(
                self.context,
                DocumentSetVersion,
                ResourcePath("documentSetVersions", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "contentType": self.content_type,
                "documentSetVersions": self.document_set_versions,
                "driveItem": self.drive_item,
            }
            default_value = property_mapping.get(name, None)
        return super(ListItem, self).get_property(name, default_value)
