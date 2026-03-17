from office365.runtime.client_value import ClientValue


class PeoplePickerQuerySettings(ClientValue):
    """Represents additional settings for the principal query."""

    def __init__(self, exclude_all_users_on_tenant_claim=None, is_sharing=None):
        """
        :param bool exclude_all_users_on_tenant_claim: Specifies whether the all users on tenant claim provider
            is excluded or not from the principal query.
        :param bool is_sharing: Specifies if the principal query is for sharing scenario or not.
        """
        self.ExcludeAllUsersOnTenantClaim = exclude_all_users_on_tenant_claim
        self.IsSharing = is_sharing

    @property
    def entity_type_name(self):
        return "SP.UI.ApplicationPages.PeoplePickerQuerySettings"
