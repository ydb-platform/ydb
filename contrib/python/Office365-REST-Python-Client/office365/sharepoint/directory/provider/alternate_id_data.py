from office365.runtime.client_value import ClientValue


class AlternateIdData(ClientValue):
    """"""

    def __init__(self, email=None, identifying_property=None, user_principal_name=None):
        self.Email = email
        self.IdentifyingProperty = identifying_property
        self.UserPrincipalName = user_principal_name

    @property
    def entity_type_name(self):
        return "SP.Directory.Provider.AlternateIdData"
