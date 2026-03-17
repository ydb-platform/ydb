from office365.runtime.client_value import ClientValue


class TranslationJobInfo(ClientValue):
    """The TranslationJobInfo type contains information about a previously submitted translation job."""

    @property
    def entity_type_name(self):
        return "SP.Translation.TranslationJobInfo"
