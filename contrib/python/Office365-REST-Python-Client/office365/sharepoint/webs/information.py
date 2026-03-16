from datetime import datetime
from typing import Optional

from office365.sharepoint.entity import Entity


class WebInformation(Entity):
    """Specifies metadata about a site"""

    def __repr__(self):
        return self.server_relative_url or self.entity_type_name

    @property
    def configuration(self):
        # type: () -> Optional[str]
        """Specifies the identifier (ID) of the site definition configuration that was used to create the site"""
        return self.properties.get("Configuration", None)

    @property
    def created(self):
        # type: () -> Optional[datetime]
        """Specifies when the site (2) was created."""
        return self.properties.get("Created", datetime.min)

    @property
    def description(self):
        # type: () -> Optional[str]
        """Specifies the description for the site"""
        return self.properties.get("Description", None)

    @property
    def id(self):
        # type: () -> Optional[str]
        """Specifies the site identifier for the site"""
        return self.properties.get("Id", None)

    @property
    def language(self):
        # type: () -> Optional[int]
        """Specifies the language code identifier (LCID) for the language that is used on the site"""
        return self.properties.get("Language", None)

    @property
    def last_item_modified_date(self):
        # type: () -> Optional[datetime]
        """Gets the date and time that an item was last modified in the site by a non-system update.
        A non-system update is a change to a list item that is visible to end users."""
        return self.properties.get("LastItemModifiedDate", datetime.min)

    @property
    def server_relative_url(self):
        # type: () -> Optional[str]
        """Specifies the server-relative URL of the site"""
        return self.properties.get("ServerRelativeUrl", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """Specifies the title for the site. Its length MUST be equal to or less than 255."""
        return self.properties.get("Title", None)

    @property
    def web_template(self):
        # type: () -> Optional[str]
        """Specifies the name of the site template that was used to create the site."""
        return self.properties.get("WebTemplate", None)

    @property
    def web_template_id(self):
        # type: () -> Optional[int]
        """Specifies the identifier of the site template that was used to create the site"""
        return self.properties.get("WebTemplateId", None)
