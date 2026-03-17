from office365.runtime.client_value import ClientValue
from office365.sharepoint.sharing.links.info import SharingLinkInfo


class SharingLinkDefaultTemplate(ClientValue):
    """"""

    def __init__(self, link_details=SharingLinkInfo()):
        self.linkDetails = link_details
