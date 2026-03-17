from office365.runtime.client_value import ClientValue


class PickerEntityInformationRequest(ClientValue):
    """Represents a request for GetPickerEntityInformation"""

    def __init__(self, email_address=None, group_id=None, key=None, principal_type=0):
        """
        :param str email_address: Gets or sets the email address of the principal.
        :param str group_id: Gets or sets the SharePoint group Id.
        :param str key: Gets or sets the identifier of the principal.
        :param int principal_type: Gets or sets the type of the principal.
        """
        super(PickerEntityInformationRequest, self).__init__()
        self.EmailAddress = email_address
        self.GroupId = group_id
        self.Key = key
        self.PrincipalType = principal_type

    @property
    def entity_type_name(self):
        return "SP.UI.ApplicationPages.PickerEntityInformationRequest"
