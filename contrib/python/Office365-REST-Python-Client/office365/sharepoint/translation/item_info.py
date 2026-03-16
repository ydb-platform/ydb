from office365.runtime.client_value import ClientValue


class TranslationItemInfo(ClientValue):
    """The TranslationItemInfo type contains information about a previously submitted translation item."""

    def __init__(self, translation_id=None):
        """
        :param str translation_id: If this translation item belongs to an immediate translation job,
            this property MUST be ignored. Otherwise, this property contains an identifier uniquely identifying
            this translation item.
        """
        super(TranslationItemInfo, self).__init__()
        self.TranslationId = translation_id

    @property
    def entity_type_name(self):
        return "SP.Translation.TranslationItemInfo"
