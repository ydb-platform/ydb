from office365.onedrive.columns.display_name_localization import DisplayNameLocalization
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class ColumnValidation(ClientValue):
    """Represents properties that validates column values."""

    def __init__(self, formula=None, descriptions=None, default_language=None):
        """
        :param str formula: The formula to validate column value.
        :param list[DisplayNameLocalization] descriptions: The formula to validate column value.
        :param str default_language: The formula to validate column value.
        """
        self.formula = formula
        self.descriptions = ClientValueCollection(DisplayNameLocalization, descriptions)
        self.defaultLanguage = default_language
