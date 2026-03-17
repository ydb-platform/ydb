from office365.runtime.client_value import ClientValue


class SitePageVersionInfo(ClientValue):
    """Represents the version information for a given SitePage."""

    @property
    def entity_type_name(self):
        return "SP.Publishing.SitePageVersionInfo"
