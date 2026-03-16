from typing import Optional

from office365.sharepoint.fields.number import FieldNumber


class FieldCurrency(FieldNumber):
    """Specifies a field that contains currency values. To set properties, call the Update method
    (section 3.2.5.43.2.1.5)."""

    @property
    def currency_locale_id(self):
        # type: () -> Optional[int]
        """Gets the language code identifier (LCID) used to format the value of the field."""
        return self.properties.get("CurrencyLocaleId", None)

    @currency_locale_id.setter
    def currency_locale_id(self, value):
        # type: (int) -> None
        """Sets the language code identifier (LCID) used to format the value of the field."""
        self.set_property("CurrencyLocaleId", value)
