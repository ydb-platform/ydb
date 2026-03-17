from office365.runtime.client_value import ClientValue


class SharingInformationRequest(ClientValue):
    """Represents the optional Request Object for GetSharingInformation."""

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharingInformationRequest"
