from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.apps.license import AppLicense


class AppLicenseCollection(ClientValue):
    """Specifies a collection of marketplace licenses."""

    def __init__(self, items=ClientValueCollection(AppLicense)):
        self.Items = items
