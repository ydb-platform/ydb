from office365.sharepoint.entity import Entity


class PointPublishingSiteStatus(Entity):
    @property
    def entity_type_name(self):
        return "SP.Publishing.PointPublishingSiteStatus"
