from office365.onedrive.contenttypes.info import ContentTypeInfo
from office365.onedrive.documentsets.content import DocumentSetContent
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class DocumentSet(ClientValue):
    """Represents a document set in SharePoint."""

    def __init__(
        self,
        welcome_page_url=None,
        allowed_content_types=None,
        default_contents=None,
        propagate_welcome_page_changes=None,
        should_prefix_name_to_file=None,
    ):
        """
        :param str welcome_page_url:  Welcome page absolute URL.
        :param list[ContentTypeInfo] allowed_content_types:  Content types allowed in document set.
        :param list[DocumentSetContent] default_contents:  Default contents of document set.
        :param bool propagate_welcome_page_changes:  Specifies whether to push welcome page changes to inherited
            content types.
        :param bool should_prefix_name_to_file:  Indicates whether to add the name of the document set to each file name.
        """
        self.welcomePageUrl = welcome_page_url
        self.allowedContentTypes = ClientValueCollection(
            ContentTypeInfo, allowed_content_types
        )
        self.defaultContents = ClientValueCollection(
            DocumentSetContent, default_contents
        )
        self.propagateWelcomePageChanges = propagate_welcome_page_changes
        self.shouldPrefixNameToFile = should_prefix_name_to_file
