from office365.runtime.client_value import ClientValue


class Refiner(ClientValue):
    """A refiner contains a list with entries, of the RefinerEntry types"""

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.Refiner"
