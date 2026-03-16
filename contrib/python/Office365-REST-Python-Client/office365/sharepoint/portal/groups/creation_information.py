from office365.runtime.client_value import ClientValue
from office365.sharepoint.portal.groups.creation_params import GroupCreationParams


class GroupCreationInformation(ClientValue):
    def __init__(self, display_name, alias, is_public, optional_params=None):
        super(GroupCreationInformation, self).__init__()
        if optional_params is None:
            optional_params = GroupCreationParams()
        self.displayName = display_name
        self.alias = alias
        self.isPublic = is_public
        self.optionalParams = optional_params

    @property
    def entity_type_name(self):
        return None
