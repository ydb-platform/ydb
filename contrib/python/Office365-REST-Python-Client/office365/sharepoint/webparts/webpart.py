from typing import Optional

from office365.sharepoint.entity import Entity


class WebPart(Entity):
    """
    A reusable component that contains or generates web-based content such as XML, HTML, and scripting code.
    It has a standard property schema and displays that content in a cohesive unit on a webpage. See also Web Parts Page
    """

    def __str__(self):
        return self.title or self.entity_type_name

    @property
    def export_mode(self):
        # type: () -> Optional[int]
        """
        Gets or sets the export mode of a Web Part.
        """
        return self.properties.get("ExportMode", None)

    @property
    def hidden(self):
        # type: () -> Optional[bool]
        """
        Specifies whether a Web Part is displayed on a Web Part Page.

        If "true", the Web Part MUST be hidden. Web Parts that are hidden MUST NOT be displayed on the page to
        the end user, but SHOULD still participate normally in the rendering of the page. The default value is "false".
        """
        return self.properties.get("Hidden", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """
        Specifies the title of a Web Part.

        If this property is set to NULL, the title of the Web Part MUST instead be reset to its default value if one
        is specified in the Web Part's definition.
        """
        return self.properties.get("Title", None)

    @property
    def title_url(self):
        # type: () -> Optional[str]
        """
        Specifies a URL to the supplemental information about a Web Part.

        If this property is set to NULL, the title URL of the Web Part MUST instead be reset to its default value
        if one is specified in the Web Part's definition.

         The value of this property SHOULD be a valid URL, but the protocol server MAY accept other strings
         without validating them.
        """
        return self.properties.get("TitleUrl", None)

    @property
    def zone_index(self):
        # type: () -> Optional[int]
        """
        An integer that specifies the relative position of a Web Part in a Web Part zone.
        Web Parts are positioned from the smallest to the largest zone index. If two or more Web Parts have the
        same zone index they are positioned adjacent to each other in an undefined order
        """
        return self.properties.get("ZoneIndex", None)

    @property
    def entity_type_name(self):
        return "SP.WebParts.WebPart"
