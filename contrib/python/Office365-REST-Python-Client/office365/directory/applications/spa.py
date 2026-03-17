from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class SpaApplication(ClientValue):
    """Specifies settings for a single-page application."""

    def __init__(self, redirect_uris=None):
        self.redirectUris = StringCollection(redirect_uris)
