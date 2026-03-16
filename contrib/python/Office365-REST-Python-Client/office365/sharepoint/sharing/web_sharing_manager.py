from office365.sharepoint.entity import Entity


class WebSharingManager(Entity):
    """Specifies a placeholder for all web sharing methods."""

    @property
    def entity_type_name(self):
        return "SP.Sharing.WebSharingManager"
