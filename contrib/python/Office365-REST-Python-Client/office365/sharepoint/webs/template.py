from typing import Optional

from office365.sharepoint.entity import Entity


class WebTemplate(Entity):
    """Specifies a site definition or a site template that is used to instantiate a site."""

    def __str__(self):
        return self.title or self.entity_type_name

    def __repr__(self):
        return self.name

    @property
    def description(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the description of the list template."""
        return self.properties.get("Description", None)

    @property
    def display_category(self):
        # type: () -> Optional[str]
        """
        Specifies the display name for the category that this site definition configuration or site template is
        a part of.
        """
        return self.properties.get("DisplayCategory", None)

    @property
    def image_url(self):
        # type: () -> Optional[str]
        """
        Specifies the URL for the image that is associated with the site definition configuration or site template.
        """
        return self.properties.get("ImageUrl", None)

    @property
    def is_hidden(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the site definition configuration is displayed in the user interface for creating new sites
        """
        return self.properties.get("IsHidden", None)

    @property
    def is_root_web_only(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the site definition configuration or site template can only be applied to the top-level site
        in the site collection.
        """
        return self.properties.get("IsRootWebOnly", None)

    @property
    def is_sub_web_only(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the site definition configuration or site template can only be applied to subsites
        created within the site collection.
        """
        return self.properties.get("IsSubWebOnly", None)

    @property
    def lcid(self):
        # type: () -> Optional[int]
        """Specifies the LCID for the site definition configuration or site template."""
        return self.properties.get("Lcid", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the display name of the list template."""
        return self.properties.get("Name", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """Specifies the display name for the site definition configuration or site template."""
        return self.properties.get("Title", None)
