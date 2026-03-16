from typing import Optional

from office365.entity_collection import EntityCollection
from office365.onedrive.base_item import BaseItem
from office365.onedrive.columns.column_link import ColumnLink
from office365.onedrive.columns.definition import ColumnDefinition
from office365.onedrive.contenttypes.order import ContentTypeOrder
from office365.onedrive.documentsets.content import DocumentSetContent
from office365.onedrive.documentsets.document_set import DocumentSet
from office365.onedrive.listitems.item_reference import ItemReference
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection


class ContentType(BaseItem):
    """The contentType resource represents a content type in SharePoint. Content types allow you to define a set of
    columns that must be present on every listItem in a list."""

    def is_published(self):
        """Check the publishing status of a contentType in a content type hub site."""
        return_type = ClientResult(self.context, bool())
        qry = FunctionQuery(self, "isPublished", None, return_type)
        self.context.add_query(qry)
        return return_type

    def associate_with_hub_sites(
        self, hub_site_urls, propagate_to_existing_lists=False
    ):
        """
        Associate a published content type present in a content type hub with a list of hub sites.
        Note: This feature is limited to tenants that have a SharePoint Syntex license.

        :param list[str] hub_site_urls: List of canonical URLs to the hub sites where the content type needs to
            be enforced. Required.
        :param bool propagate_to_existing_lists: If true, content types will be enforced on existing lists in the
            hub sites; otherwise, it'll be applied only to newly created lists.
        """
        payload = {
            "hubSiteUrls": StringCollection(hub_site_urls),
            "propagateToExistingLists": propagate_to_existing_lists,
        }
        qry = ServiceOperationQuery(self, "associateWithHubSites", None, payload)
        self.context.add_query(qry)
        return self

    def publish(self):
        """
        Publishes a contentType present in the content type hub site.
        """
        qry = ServiceOperationQuery(self, "publish")
        self.context.add_query(qry)
        return self

    def unpublish(self):
        """Unpublish a contentType from a content type hub site."""
        qry = ServiceOperationQuery(self, "unpublish")
        self.context.add_query(qry)
        return self

    @property
    def associated_hubs_urls(self):
        """
        List of canonical URLs for hub sites with which this content type is associated to.
        This will contain all hub sites where this content type is queued to be enforced or is already enforced.
        Enforcing a content type means that the content type will be applied to the lists in the enforced sites.
        """
        return self.properties.get("associatedHubsUrls", StringCollection())

    @property
    def document_set(self):
        """Document Set metadata."""
        return self.properties.get("documentSet", DocumentSet())

    @property
    def document_template(self):
        """
        Document template metadata. To make sure that documents have consistent content across a site and its subsites,
        you can associate a Word, Excel, or PowerPoint template with a site content type.
        """
        return self.properties.get("documentTemplate", DocumentSetContent())

    @property
    def name(self):
        # type: () -> Optional[str]
        """The name of the content type."""
        return self.properties.get("name", None)

    @property
    def description(self):
        # type: () -> Optional[str]
        """The descriptive text for the item."""
        return self.properties.get("description", None)

    @property
    def parent_id(self):
        # type: () -> Optional[str]
        """The unique identifier of the content type."""
        return self.properties.get("parentId", None)

    @property
    def propagate_changes(self):
        # type: () -> Optional[bool]
        """If 'true', changes to this column will be propagated to lists that implement the column."""
        return self.properties.get("propagateChanges", None)

    @property
    def read_only(self):
        # type: () -> Optional[bool]
        """If true, the content type cannot be modified unless this value is first set to false."""
        return self.properties.get("readOnly", None)

    @property
    def inherited_from(self):
        """
        If this content type is inherited from another scope (like a site),
        provides a reference to the item where the content type is defined.
        """
        return self.properties.get("inheritedFrom", ItemReference())

    @property
    def column_links(self):
        # type: () -> EntityCollection[ColumnLink]
        """The collection of columns that are required by this content type"""
        return self.properties.get(
            "columnLinks",
            EntityCollection(
                self.context,
                ColumnLink,
                ResourcePath("columnLinks", self.resource_path),
            ),
        )

    @property
    def base(self):
        """Parent contentType from which this content type is derived."""
        return self.properties.get(
            "base", ContentType(self.context, ResourcePath(self.resource_path))
        )

    @property
    def base_types(self):
        # type: () -> EntityCollection["ContentType"]
        """The collection of content types that are ancestors of this content type."""
        return self.properties.get(
            "baseTypes",
            EntityCollection(
                self.context, ContentType, ResourcePath("baseTypes", self.resource_path)
            ),
        )

    @property
    def columns(self):
        # type: () -> EntityCollection[ColumnDefinition]
        """The collection of column definitions for this contentType."""
        return self.properties.get(
            "columns",
            EntityCollection(
                self.context,
                ColumnDefinition,
                ResourcePath("columns", self.resource_path),
            ),
        )

    @property
    def column_positions(self):
        # type: () -> EntityCollection[ColumnDefinition]
        """Column order information in a content type."""
        return self.properties.get(
            "columnPositions",
            EntityCollection(
                self.context,
                ColumnDefinition,
                ResourcePath("columnPositions", self.resource_path),
            ),
        )

    @property
    def order(self):
        """Specifies the order in which the content type appears in the selection UI."""
        return self.properties.get("order", ContentTypeOrder())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "columnLinks": self.column_links,
                "documentSet": self.document_set,
                "documentTemplate": self.document_template,
                "columnPositions": self.column_positions,
                "baseTypes": self.base_types,
                "inheritedFrom": self.inherited_from,
            }
            default_value = property_mapping.get(name, None)
        return super(ContentType, self).get_property(name, default_value)
