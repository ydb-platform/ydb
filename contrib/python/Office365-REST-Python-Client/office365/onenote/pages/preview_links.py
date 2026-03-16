from office365.onenote.pages.external_link import ExternalLink
from office365.runtime.client_value import ClientValue


class OnenotePagePreviewLinks(ClientValue):
    """"""

    def __init__(self, preview_image_url=ExternalLink()):
        self.previewImageUrl = preview_image_url
