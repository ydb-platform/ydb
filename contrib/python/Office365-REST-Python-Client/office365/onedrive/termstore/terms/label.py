from office365.runtime.client_value import ClientValue


class LocalizedLabel(ClientValue):
    """
    Represents the localized name used in the term store, which identifies the name in the localized language.
    For more information, see localizedLabel.
    """

    def __init__(self, name=None, language_tag="en-US", is_default=True):
        """
        :param str name: The name in the localized language.
        :param str language_tag: The language tag for the label.
        :param bool is_default: Indicates whether the label is the default label.
        """
        super(LocalizedLabel, self).__init__()
        self.name = name
        self.languageTag = language_tag
        self.isDefault = is_default
