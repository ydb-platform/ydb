from office365.entity import Entity
from office365.search.external.configuration import Configuration
from office365.search.external.search_settings import SearchSettings


class ExternalConnection(Entity):
    """A logical container to add content from an external source into Microsoft Graph."""

    @property
    def configuration(self):
        """
        Specifies additional application IDs that are allowed to manage the connection and to index
        content in the connection. Optional.
        """
        return self.properties.get("configuration", Configuration())

    @property
    def search_settings(self):
        """
        The settings configuring the search experience for content in this connection,
        such as the display templates for search results.
        """
        return self.properties.get("searchSettings", SearchSettings())

    @property
    def entity_type_name(self):
        return "microsoft.graph.externalConnectors.externalConnection"
