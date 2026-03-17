from office365.runtime.client_value import ClientValue


class PortalLaunchWave(ClientValue):
    @property
    def entity_type_name(self):
        return "SP.Publishing.PortalLaunch.PortalLaunchWave"
