from office365.runtime.client_value import ClientValue


class SitePageStreamData(ClientValue):

    @property
    def entity_type_name(self):
        return "SP.Publishing.SitePageStreamData"
