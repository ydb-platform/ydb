from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.sharing.links.default_template import (
    SharingLinkDefaultTemplate,
)


class SharingLinkDefaultTemplatesCollection(ClientValue):
    def __init__(self, templates=None):
        self.templates = ClientValueCollection(SharingLinkDefaultTemplate, templates)
