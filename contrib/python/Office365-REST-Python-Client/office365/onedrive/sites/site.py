from datetime import datetime
from typing import Optional

from office365.entity_collection import EntityCollection
from office365.onedrive.analytics.item_activity_stat import ItemActivityStat
from office365.onedrive.analytics.item_analytics import ItemAnalytics
from office365.onedrive.base_item import BaseItem
from office365.onedrive.columns.definition_collection import ColumnDefinitionCollection
from office365.onedrive.contenttypes.collection import ContentTypeCollection
from office365.onedrive.drives.drive import Drive
from office365.onedrive.listitems.list_item import ListItem
from office365.onedrive.lists.collection import ListCollection
from office365.onedrive.operations.rich_long_running import RichLongRunningOperation
from office365.onedrive.permissions.collection import PermissionCollection
from office365.onedrive.sharepoint_ids import SharePointIds
from office365.onedrive.sitepages.collection import SitePageCollection
from office365.onedrive.sites.site_collection import SiteCollection
from office365.onedrive.termstore.store import Store
from office365.onenote.onenote import Onenote
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery


class Site(BaseItem):
    """The site resource provides metadata and relationships for a SharePoint site."""

    def __str__(self):
        return self.name or self.entity_type_name

    def __repr__(self):
        return self.name or self.web_url or self.id or self.entity_type_name

    def get_by_path(self, path):
        # type: (str) -> Site
        """
        Retrieve properties and relationships for a site resource. A site resource represents a team site in SharePoint.

            In addition to retrieving a site by ID you can retrieve a site based on server-relative URL path.

            Site collection hostname (contoso.sharepoint.com)
            Site path, relative to server hostname.
            There is also a reserved site identifier, root, which always references the root site for a given target,
            as follows:

            /sites/root: The tenant root site.
            /groups/{group-id}/sites/root: The group's team site.
        """
        return_type = Site(self.context)
        qry = ServiceOperationQuery(self, "GetByPath", [path], None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_applicable_content_types_for_list(self, list_id):
        # type: (str) -> ContentTypeCollection
        """
        Get site contentTypes that can be added to a list.

        :param str list_id: GUID of the list for which the applicable content types need to be fetched.
        """
        return_type = ContentTypeCollection(
            self.context, self.content_types.resource_path
        )
        params = {"listId": list_id}
        qry = FunctionQuery(
            self, "getApplicableContentTypesForList", params, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_activities_by_interval(self, start_dt=None, end_dt=None, interval=None):
        # type: (Optional[datetime], Optional[datetime], str) -> EntityCollection[ItemActivityStat]
        """
        Get a collection of itemActivityStats resources for the activities that took place on this resource
        within the specified time interval.

        :param datetime.datetime start_dt: The start time over which to aggregate activities.
        :param datetime.datetime end_dt: The end time over which to aggregate activities.
        :param str interval: The aggregation interval.
        """
        params = {
            "startDateTime": start_dt.strftime("%m-%d-%Y") if start_dt else None,
            "endDateTime": end_dt.strftime("%m-%d-%Y") if end_dt else None,
            "interval": interval,
        }
        return_type = EntityCollection(self.context, ItemActivityStat)
        qry = FunctionQuery(self, "getActivitiesByInterval", params, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def site_collection(self):
        # type: () -> SiteCollection
        """Provides details about the site's site collection. Available only on the root site."""
        return self.properties.get("siteCollection", SiteCollection())

    @property
    def sharepoint_ids(self):
        # type: () -> SharePointIds
        """Returns identifiers useful for SharePoint REST compatibility."""
        return self.properties.get("sharepointIds", SharePointIds())

    @property
    def items(self):
        # type: () -> EntityCollection[ListItem]
        """Used to address any item contained in this site. This collection cannot be enumerated."""
        return self.properties.get(
            "items",
            EntityCollection(
                self.context, ListItem, ResourcePath("items", self.resource_path)
            ),
        )

    @property
    def columns(self):
        # type: () -> ColumnDefinitionCollection
        """The collection of columns under this site."""
        return self.properties.get(
            "columns",
            ColumnDefinitionCollection(
                self.context, ResourcePath("columns", self.resource_path), self
            ),
        )

    @property
    def external_columns(self):
        # type: () -> ColumnDefinitionCollection
        """The collection of columns under this site."""
        return self.properties.get(
            "externalColumns",
            ColumnDefinitionCollection(
                self.context, ResourcePath("externalColumns", self.resource_path), self
            ),
        )

    @property
    def content_types(self):
        # type: () -> ContentTypeCollection
        """The collection of content types under this site."""
        return self.properties.get(
            "contentTypes",
            ContentTypeCollection(
                self.context, ResourcePath("contentTypes", self.resource_path)
            ),
        )

    @property
    def lists(self):
        # type: () -> ListCollection
        """The collection of lists under this site."""
        return self.properties.get(
            "lists",
            ListCollection(self.context, ResourcePath("lists", self.resource_path)),
        )

    @property
    def operations(self):
        # type: () -> EntityCollection[RichLongRunningOperation]
        """The collection of long-running operations on the site."""
        return self.properties.get(
            "operations",
            EntityCollection(
                self.context,
                RichLongRunningOperation,
                ResourcePath("operations", self.resource_path),
            ),
        )

    @property
    def permissions(self):
        # type: () -> PermissionCollection
        """The permissions associated with the site."""
        return self.properties.get(
            "permissions",
            PermissionCollection(
                self.context, ResourcePath("permissions", self.resource_path)
            ),
        )

    @property
    def drive(self):
        # type: () -> Drive
        """The default drive (document library) for this site."""
        return self.properties.get(
            "drive", Drive(self.context, ResourcePath("drive", self.resource_path))
        )

    @property
    def drives(self):
        # type: () -> EntityCollection[Drive]
        """The collection of drives under this site."""
        return self.properties.get(
            "drives",
            EntityCollection(
                self.context, Drive, ResourcePath("drives", self.resource_path)
            ),
        )

    @property
    def sites(self):
        # type: () -> EntityCollection[Site]
        """The collection of sites under this site."""
        return self.properties.get(
            "sites",
            EntityCollection(
                self.context, Site, ResourcePath("sites", self.resource_path)
            ),
        )

    @property
    def analytics(self):
        # type: () -> ItemAnalytics
        """Analytics about the view activities that took place on this site."""
        return self.properties.get(
            "analytics",
            ItemAnalytics(self.context, ResourcePath("analytics", self.resource_path)),
        )

    @property
    def onenote(self):
        # type: () -> Onenote
        """Represents the Onenote services available to a site."""
        return self.properties.get(
            "onenote",
            Onenote(self.context, ResourcePath("onenote", self.resource_path)),
        )

    @property
    def pages(self):
        # type: () -> SitePageCollection
        """The collection of site pages under this site."""
        return self.properties.get(
            "pages",
            SitePageCollection(self.context, ResourcePath("pages", self.resource_path)),
        )

    @property
    def term_store(self):
        # type: () -> Store
        """The default termStore under this site."""
        return self.properties.get(
            "termStore",
            Store(self.context, ResourcePath("termStore", self.resource_path)),
        )

    @property
    def term_stores(self):
        # type: () -> EntityCollection[Store]
        """The collection of termStores under this site."""
        return self.properties.get(
            "termStores",
            EntityCollection(
                self.context, Store, ResourcePath("termStores", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "contentTypes": self.content_types,
                "externalColumns": self.external_columns,
                "siteCollection": self.site_collection,
                "termStore": self.term_store,
                "termStores": self.term_stores,
            }
            default_value = property_mapping.get(name, None)
        return super(Site, self).get_property(name, default_value)
