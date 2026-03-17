from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class AnnouncementsController(Entity):
    def __init__(self, context, path=None):
        if path is None:
            path = ResourcePath("SP.Publishing.AnnouncementsController")
        super(AnnouncementsController, self).__init__(context, path)

    @property
    def entity_type_name(self):
        return "SP.Publishing.AnnouncementsController"
