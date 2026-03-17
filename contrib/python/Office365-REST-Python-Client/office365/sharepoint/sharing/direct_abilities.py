from office365.runtime.client_value import ClientValue
from office365.sharepoint.sharing.ability_status import SharingAbilityStatus


class DirectSharingAbilities(ClientValue):
    """Represents the set of capabilities for direct sharing for the current user."""

    def __init__(
        self,
        can_add_external_principal=SharingAbilityStatus(),
        can_add_internal_principal=SharingAbilityStatus(),
        can_request_grant_access=SharingAbilityStatus(),
        supports_review_permission=SharingAbilityStatus(),
    ):
        """
        :param SharingAbilityStatus can_add_external_principal: Indicates whether the current user can share to new
            external users.
        :param SharingAbilityStatus can_add_internal_principal: Indicates whether the current user can share to
            internal users.
        :param SharingAbilityStatus can_request_grant_access: Indicates whether the current user can initiate a
            request for someone with higher permissions to grant access.
        """
        self.canAddExternalPrincipal = can_add_external_principal
        self.canAddInternalPrincipal = can_add_internal_principal
        self.canRequestGrantAccess = can_request_grant_access
        self.supportsReviewPermission = supports_review_permission

    @property
    def entity_type_name(self):
        return "SP.Sharing.DirectSharingAbilities"
