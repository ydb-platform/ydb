from office365.runtime.client_value import ClientValue


class SharePointOneDriveOptions(ClientValue):
    """Provides the search content options when a search is performed using application permissions"""

    def __init__(self, search_content=None):
        self.searchContent = search_content
