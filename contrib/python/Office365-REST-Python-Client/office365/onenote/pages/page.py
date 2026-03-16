from typing import AnyStr, Optional

from office365.onenote.entity_schema_object_model import OnenoteEntitySchemaObjectModel
from office365.onenote.notebooks.notebook import Notebook
from office365.onenote.pages.links import PageLinks
from office365.onenote.sections.section import OnenoteSection
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.types.collections import StringCollection


class OnenotePage(OnenoteEntitySchemaObjectModel):
    """A page in a OneNote notebook."""

    def get_content(self):
        # type: () -> ClientResult[AnyStr]
        """Download the page's HTML content."""
        return_type = ClientResult(self.context)
        qry = FunctionQuery(self, "content", None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def content_url(self):
        # type: () -> Optional[str]
        """The URL for the page's HTML content. Read-only."""
        return self.properties.get("contentUrl", None)

    @property
    def links(self):
        """Links for opening the page. The oneNoteClientURL link opens the page in the OneNote native client
        if it 's installed. The oneNoteWebUrl link opens the page in OneNote on the web. Read-only.

        """
        return self.properties.get("links", PageLinks())

    @property
    def title(self):
        # type: () -> Optional[str]
        """The title of the page."""
        return self.properties.get("title", None)

    @property
    def user_tags(self):
        """Links for opening the page. The oneNoteClientURL link opens the page in the OneNote native client
        if it 's installed. The oneNoteWebUrl link opens the page in OneNote on the web. Read-only.

        """
        return self.properties.get("userTags", StringCollection())

    @property
    def parent_notebook(self):
        """The notebook that contains the page. Read-only."""
        return self.properties.get(
            "parentNotebook",
            Notebook(self.context, ResourcePath("parentNotebook", self.resource_path)),
        )

    @property
    def parent_section(self):
        """The section that contains the page."""
        return self.properties.get(
            "parentSection",
            OnenoteSection(
                self.context, ResourcePath("parentSection", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "userTags": self.user_tags,
                "parentSection": self.parent_section,
                "parentNotebook": self.parent_notebook,
            }
            default_value = property_mapping.get(name, None)
        return super(OnenotePage, self).get_property(name, default_value)
