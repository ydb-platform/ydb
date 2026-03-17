from office365.onenote.pages.external_link import ExternalLink
from office365.runtime.client_value import ClientValue


class RecentNotebookLinks(ClientValue):
    """
    Links for opening a OneNote notebook. This resource type exists as a property on a recentNotebook resource.
    """

    def __init__(
        self, onenote_client_url=ExternalLink(), onenote_web_url=ExternalLink()
    ):
        self.oneNoteClientUrl = onenote_client_url
        self.oneNoteWebUrl = onenote_web_url
