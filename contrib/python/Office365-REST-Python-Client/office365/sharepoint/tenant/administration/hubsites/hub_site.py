from typing import Optional

from office365.sharepoint.entity import Entity


class HubSite(Entity):
    """SharePoint hub sites help you meet the needs of your organization by connecting and organizing sites"""

    def __str__(self):
        return self.title or self.entity_type_name

    def __repr__(self):
        return self.site_url or self.entity_type_name

    @property
    def id(self):
        # type: () -> Optional[str]
        """Gets the id of the hub site."""
        return self.properties.get("ID", None)

    @property
    def description(self):
        # type: () -> Optional[str]
        """Gets the description of the hub site type."""
        return self.properties.get("Description", None)

    @property
    def site_url(self):
        # type: () -> Optional[str]
        """Gets the url of the hub site."""
        return self.properties.get("SiteUrl", None)

    @property
    def targets(self):
        # type: () -> Optional[str]
        """List of security groups with access to join the hub site. Null if everyone has permission."""
        return self.properties.get("Targets", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """Gets the title of the hub site."""
        return self.properties.get("Title", None)

    @property
    def tenant_instance_id(self):
        # type: () -> Optional[str]
        """Gets The tenant instance ID in which the hub site is located."""
        return self.properties.get("TenantInstanceId", None)
