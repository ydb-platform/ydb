from office365.runtime.client_value import ClientValue


class ScsEndpoint(ClientValue):
    """ """

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ScsEndpoint"
