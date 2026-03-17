from typing import Optional

from office365.sharepoint.fields.field import Field


class FieldCalculated(Field):
    """
    Specifies a calculated field in a list. To set properties, call the Update method (section 3.2.5.38.2.1.5).

    The NoCrawl and SchemaXmlWithResourceTokens properties are not included in the default scalar property set
        for this type.
    """

    @property
    def currency_locale_id(self):
        # type: () -> Optional[int]
        """Gets the locale ID that is used for currency on the Web site."""
        return self.properties.get("CurrencyLocaleId", None)

    @property
    def formula(self):
        # type: () -> Optional[str]
        """Specifies the formula for the field"""
        return self.properties.get("Formula", None)

    @formula.setter
    def formula(self, val):
        # type: (str) -> None
        """Sets a value that specifies the Formula."""
        self.set_property("Formula", val)
