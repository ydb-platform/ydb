from office365.runtime.client_value import ClientValue


class SPClientSideComponentQueryResult(ClientValue):
    """This object contains information about the requested component and the status of the query that was used to
    retrieve the component."""

    def __init__(self, component_type=None, manifest=None, manifest_type=None):
        """
        :param str component_type: Specifies the type of component.
        :param str manifest:
        :param str manifest_type:
        """
        self.ComponentType = component_type
        self.Manifest = manifest
        self.ManifestType = manifest_type
