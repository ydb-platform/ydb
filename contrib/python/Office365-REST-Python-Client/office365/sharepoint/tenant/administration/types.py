from office365.runtime.client_value import ClientValue


class DisableGroupify(ClientValue):
    """ """

    def __init__(self, is_read_only=None, value=None):
        # type: (bool, bool) -> None
        self.IsReadOnly = is_read_only
        self.Value = value

    def __repr__(self):
        return f"(IsReadOnly={self.IsReadOnly}, Value={self.Value})"

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.DisableGroupify"


class EnableAutoNewsDigest(ClientValue):
    """ """

    def __init__(self, is_read_only=None, value=None):
        # type: (bool, bool) -> None
        self.IsReadOnly = is_read_only
        self.Value = value

    def __repr__(self):
        return f"(IsReadOnly={self.IsReadOnly}, Value={self.Value})"

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.EnableAutoNewsDigest"


class DisableSelfServiceSiteCreation(ClientValue):
    """ """

    def __init__(self, is_read_only=None, value=None):
        # type: (bool, bool) -> None
        self.IsReadOnly = is_read_only
        self.Value = value

    def __repr__(self):
        return f"(IsReadOnly={self.IsReadOnly}, Value={self.Value})"

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.DisableSelfServiceSiteCreation"


class AutoQuotaEnabled(ClientValue):
    """Automatic quota management type"""

    def __init__(self, is_read_only=None, value=None):
        self.IsReadOnly = is_read_only
        self.Value = value

    def __repr__(self):
        return f"(IsReadOnly={self.IsReadOnly}, Value={self.Value})"

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.AutoQuotaEnabled"


class CreatePolicyRequest(ClientValue):
    """ """

    def __init__(
        self,
        is_preview_run=None,
        policy_custom_name=None,
        policy_definition_details=None,
        policy_description=None,
        policy_frequency_unit=None,
        policy_frequency_value=None,
        policy_id=None,
        policy_tags=None,
        policy_template=None,
        policy_type=None,
    ):
        """
        :param bool is_preview_run:
        :param str policy_custom_name:
        :param str policy_definition_details:
        :param str policy_description:
        :param int policy_frequency_unit:
        :param int policy_frequency_value:
        :param str policy_id:
        :param str policy_tags:
        :param str policy_template:
        :param int policy_type:
        """
        self.isPreviewRun = is_preview_run
        self.policyCustomName = policy_custom_name
        self.policyDefinitionDetails = policy_definition_details
        self.policyDescription = policy_description
        self.policyFrequencyUnit = policy_frequency_unit
        self.policyFrequencyValue = policy_frequency_value
        self.policyId = policy_id
        self.policyTags = policy_tags
        self.policyTemplate = policy_template
        self.policyType = policy_type

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.CreatePolicyRequest"
