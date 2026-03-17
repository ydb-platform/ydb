from typing import IO

from office365.entity_collection import EntityCollection
from office365.onenote.internal.multipart_page_query import OneNotePageCreateQuery
from office365.onenote.pages.page import OnenotePage


class OnenotePageCollection(EntityCollection[OnenotePage]):
    """A collection of pages in a OneNote notebook"""

    def __init__(self, context, resource_path=None):
        super(OnenotePageCollection, self).__init__(context, OnenotePage, resource_path)

    def add(self, presentation_file, attachment_files=None):
        # type: (IO, dict) -> OnenotePage
        """
        Create a new OneNote page.
        :param typing.IO presentation_file: Presentation file
        :param dict or None attachment_files: Attachment files
        """
        qry = OneNotePageCreateQuery(self, presentation_file, attachment_files)
        self.context.add_query(qry)
        return qry.return_type
