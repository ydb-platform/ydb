from office365.directory.permissions.identity import Identity
from office365.runtime.client_value import ClientValue


class SharingLink(ClientValue):
    """The SharingLink resource groups link-related data items into a single structure."""

    def __init__(
        self,
        _type=None,
        scope=None,
        web_html=None,
        web_url=None,
        prevents_download=None,
        application=Identity(),
    ):
        """
        :param str _type: The type of the link created.
        :param str scope: The scope of the link represented by this permission. Value anonymous indicates the link is
             usable by anyone, organization indicates the link is only usable for users signed into the same tenant.
         :param str web_html: For embed links, this property contains the HTML code for an <iframe> element that will
             embed the item in a webpage.
         :param str web_url: A URL that opens the item in the browser on the OneDrive website.
         :param bool prevents_download: If true then the user can only use this link to view the item on the web,
              and cannot use it to download the contents of the item. Only for OneDrive for Business and SharePoint.
         :param Identity application: The app the link is associated with.
        """
        super(SharingLink, self).__init__()
        self.type = _type
        self.scope = scope
        self.webHtml = web_html
        self.webUrl = web_url
        self.preventsDownload = prevents_download
        self.application = application

    def __repr__(self):
        return self.webUrl or ""
