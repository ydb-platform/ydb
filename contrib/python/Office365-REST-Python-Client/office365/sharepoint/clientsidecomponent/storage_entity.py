from office365.sharepoint.entity import Entity


class StorageEntity(Entity):
    """Storage entities which are available across app catalog scopes."""

    @property
    def value(self):
        """The value inside the storage entity."""
        return self.properties.get("Value", None)
