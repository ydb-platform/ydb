from office365.onenote.pages.external_link import ExternalLink
from office365.runtime.client_value import ClientValue


class PageLinks(ClientValue):
    """Links for opening a OneNote page."""

    def __init__(
        self, onenote_client_url=ExternalLink(), onenote_web_url=ExternalLink()
    ):
        """
        :param ExternalLink onenote_client_url: Opens the page in the OneNote native client if it's installed.
        :param ExternalLink onenote_web_url: Opens the page in OneNote on the web.
        """
        super(PageLinks, self).__init__()
        self.oneNoteClientUrl = onenote_client_url
        self.oneNoteWebUrl = onenote_web_url
