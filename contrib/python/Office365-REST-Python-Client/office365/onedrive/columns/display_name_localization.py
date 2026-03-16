from office365.runtime.client_value import ClientValue


class DisplayNameLocalization(ClientValue):
    """Provides the ability for an administrator to customize the string used in a shared Microsoft 365 experience."""

    def __init__(self, display_name=None, language_tag=None):
        """
        :param str display_name: If present, the value of this field contains the displayName string that has been
            set for the language present in the languageTag field.
        :param str language_tag: Provides the language culture-code and friendly name of the language that the
            displayName field has been provided in.
        """
        self.displayName = display_name
        self.languageTag = language_tag
