from office365.sharepoint.entity import Entity


class SecurableObjectExtensions(Entity):
    """Contains extension methods of securable object."""

    @property
    def entity_type_name(self):
        return "SP.Sharing.SecurableObjectExtensions"
