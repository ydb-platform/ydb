from office365.runtime.client_value import ClientValue


class GroupCreationContext(ClientValue):

    def __init__(
        self,
        preferred_language=None,
        sensitivity_label_policy_mandatory=None,
        site_path=None,
    ):
        self.PreferredLanguage = preferred_language
        self.SensitivityLabelPolicyMandatory = sensitivity_label_policy_mandatory
        self.SitePath = site_path

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.GroupCreationContext"
