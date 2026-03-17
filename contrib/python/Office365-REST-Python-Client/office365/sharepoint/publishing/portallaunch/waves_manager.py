from office365.sharepoint.entity import Entity


class WavesManager(Entity):
    @property
    def entity_type_name(self):
        return "SP.Publishing.PortalLaunch.WavesManager"
