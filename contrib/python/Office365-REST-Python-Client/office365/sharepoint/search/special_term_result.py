from office365.runtime.client_value import ClientValue


class SpecialTermResult(ClientValue):
    """Represents a row in the Table property of a SpecialTermResults Table"""

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SpecialTermResult"
