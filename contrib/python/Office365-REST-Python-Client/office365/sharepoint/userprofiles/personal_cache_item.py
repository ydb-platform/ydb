from office365.runtime.client_value import ClientValue


class PersonalCacheItem(ClientValue):
    """Object representing a PersonalCache item, returned from ReadCache methods of the PersonalCache."""

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.PersonalCacheItem"
