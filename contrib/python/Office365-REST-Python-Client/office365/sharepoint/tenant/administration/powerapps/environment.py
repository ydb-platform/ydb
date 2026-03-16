from office365.runtime.client_value import ClientValue


class PowerAppsEnvironment(ClientValue):
    """ """

    def __init__(
        self,
        AllocatedAICredits=None,
        DisplayName=None,
        IsDefault=None,
        Name=None,
        PurchasedAICredits=None,
    ):
        # type: (float, str, bool, str, float) -> None
        self.AllocatedAICredits = AllocatedAICredits
        self.DisplayName = DisplayName
        self.IsDefault = IsDefault
        self.Name = Name
        self.PurchasedAICredits = PurchasedAICredits

    def __str__(self):
        return self.DisplayName or self.entity_type_name

    def __repr__(self):
        return self.Name or self.entity_type_name

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.Online.SharePoint.TenantAdministration.PowerAppsEnvironment"
