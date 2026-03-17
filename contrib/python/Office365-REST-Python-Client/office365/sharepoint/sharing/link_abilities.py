from office365.runtime.client_value import ClientValue
from office365.sharepoint.sharing.ability_status import SharingAbilityStatus


class SharingLinkAbilities(ClientValue):
    """
    Represents the set of capabilities for specific configurations of tokenized sharing link for the current user
    and whether they are enabled or not.
    """

    def __init__(
        self,
        can_add_new_external_principals=SharingAbilityStatus(),
        can_delete_edit_link=SharingAbilityStatus(),
        can_delete_manage_list_link=SharingAbilityStatus(),
        can_get_edit_link=SharingAbilityStatus(),
        can_get_read_link=SharingAbilityStatus(),
    ):
        """
        :param SharingAbilityStatus can_add_new_external_principals:
        :param SharingAbilityStatus can_delete_edit_link:
        :param SharingAbilityStatus can_get_edit_link: Indicates whether the current user can get an existing tokenized
            sharing link that provides edit access.
        :param SharingAbilityStatus can_get_read_link: Indicates whether the current user can get an existing tokenized
            sharing link that provides read access.
        """
        self.canAddNewExternalPrincipals = can_add_new_external_principals
        self.canDeleteEditLink = can_delete_edit_link
        self.canDeleteManageListLink = can_delete_manage_list_link
        self.canGetEditLink = can_get_edit_link
        self.canGetReadLink = can_get_read_link

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharingLinkAbilities"
