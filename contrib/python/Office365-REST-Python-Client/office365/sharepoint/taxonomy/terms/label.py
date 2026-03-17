from office365.runtime.client_value import ClientValue


class Label(ClientValue):
    """"""

    def __init__(self, name=None, is_default=None, language_tag=None):
        """
        :param str name: 	Gets the value of the current Label object.
        :param bool is_default: Indicates whether this Label object is the default label for the label's language.
        :param str language_tag: Indicates the locale of the current Label object.
        """
        self.name = name
        self.isDefault = is_default
        self.languageTag = language_tag

    def __str__(self):
        return self.name

    def __repr__(self):
        return "{0}:{1}".format(self.languageTag, self.name)
