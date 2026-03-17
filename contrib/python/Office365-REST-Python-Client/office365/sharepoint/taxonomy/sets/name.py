from office365.runtime.client_value import ClientValue


class LocalizedName(ClientValue):
    """
    Represents the localized name used in the term store, which identifies the name in the localized language.
    For more information, see localizedLabel.
    """

    def __init__(self, name=None, language_tag="en-US"):
        """
        :param str name: The name in the localized language.
        :param str language_tag: The language tag for the label.
        """
        super(LocalizedName, self).__init__()
        self.name = name
        self.languageTag = language_tag

    def __repr__(self):
        return "{0};{1}".format(self.name, self.languageTag)

    def __str__(self):
        return self.name
