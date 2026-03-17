from typing import Optional

from office365.entity_collection import EntityCollection
from office365.onedrive.sitepages.base import BaseSitePage
from office365.onedrive.sitepages.canvas_layout import CanvasLayout
from office365.onedrive.sitepages.title_area import TitleArea
from office365.onedrive.sitepages.webparts.web_part import WebPart
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery


class SitePage(BaseSitePage):
    """This resource represents a page in the sitePages list. It contains the title, layout, and a collection of
    webParts."""

    def get_web_parts_by_position(
        self,
        web_part_index=None,
        horizontal_section_id=None,
        is_in_vertical_section=None,
        column_id=None,
    ):
        """
        Get a collection of webPart by providing webPartPosition information.

        :param float web_part_index: Index of the current WebPart. Represents the order of WebPart in this column or
             section. Only works if either columnId or isInVerticalSection is provided.
        :param float horizontal_section_id: Indicate the horizontal section where the WebPart located in.
        :param bool is_in_vertical_section: Indicate whether the WebPart located in the vertical section.
        :param float column_id: Indicate the identifier of the column where the WebPart located in. Only works
             if horizontalSectionId is provided.
        """
        params = {
            "webPartIndex": web_part_index,
            "horizontalSectionId": horizontal_section_id,
            "isInverticalSection": is_in_vertical_section,
            "columnId": column_id,
        }
        return_type = EntityCollection(
            self.context, WebPart, ResourcePath("webParts", self.resource_path)
        )
        qry = FunctionQuery(self, "getWebPartsByPosition", params, return_type)
        self.context.add_query(qry)
        return return_type

    def checkin(self):
        """
        Check in the latest version of a sitePage resource, which makes the version of the page available to all users.
        If the page is checked out, check in the page and publish it. If the page is checked out to the caller
        of this API, the page is automatically checked in and then published."""
        return self

    def publish(self):
        """
        Publish the latest version of a sitePage resource, which makes the version of the page available to all users.
        If the page is checked out, check in the page and publish it. If the page is checked out to the caller
        of this API, the page is automatically checked in and then published.

        If a page approval flow has been activated in the page library, the page is not published until the approval
        flow is completed.
        """
        qry = ServiceOperationQuery(self, "microsoft.graph.sitePage/publish")
        self.context.add_query(qry)
        return self

    @property
    def promotion_kind(self):
        # type: () -> Optional[str]
        """Indicates the promotion kind of the sitePage."""
        return self.properties.get("promotionKind", None)

    @property
    def show_comments(self):
        # type: () -> Optional[bool]
        """Determines whether or not to show comments at the bottom of the page."""
        return self.properties.get("showComments", None)

    @property
    def show_recommended_pages(self):
        # type: () -> Optional[bool]
        """Determines whether or not to show recommended pages at the bottom of the page."""
        return self.properties.get("showRecommendedPages", None)

    @property
    def thumbnail_web_url(self):
        # type: () -> Optional[str]
        """Indicates the promotion kind of the sitePage."""
        return self.properties.get("thumbnailWebUrl", None)

    @property
    def title_area(self):
        # type: () -> Optional[str]
        """Title area on the SharePoint page."""
        return self.properties.get("titleArea", TitleArea())

    @property
    def canvas_layout(self):
        # type: () -> CanvasLayout
        """The default termStore under this site."""
        return self.properties.get(
            "canvasLayout",
            CanvasLayout(
                self.context, ResourcePath("canvasLayout", self.resource_path)
            ),
        )

    @property
    def web_parts(self):
        # type: () -> EntityCollection[WebPart]
        """Collection of webparts on the SharePoint page."""
        return self.properties.get(
            "webParts",
            EntityCollection(
                self.context, WebPart, ResourcePath("webParts", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "canvasLayout": self.canvas_layout,
                "titleArea": self.title_area,
                "webParts": self.web_parts,
            }
            default_value = property_mapping.get(name, None)
        return super(SitePage, self).get_property(name, default_value)
