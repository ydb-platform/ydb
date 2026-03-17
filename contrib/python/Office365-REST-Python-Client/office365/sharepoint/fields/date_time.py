from typing import Optional

from office365.sharepoint.fields.field import Field


class FieldDateTime(Field):
    """Specifies a field that contains date and time values. To set properties, call the Update method
    (section 3.2.5.44.2.1.5)."""

    @property
    def datetime_calendar_type(self):
        # type: () -> Optional[int]
        """Gets the calendar type of the field"""
        return self.properties.get("DateTimeCalendarType", None)

    @datetime_calendar_type.setter
    def datetime_calendar_type(self, value):
        # type: (int) -> None
        """Sets Gets the calendar type of the field"""
        self.set_property("DateTimeCalendarType", value)

    @property
    def date_format(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("DateFormat", None)

    @property
    def display_format(self):
        # type: () -> Optional[int]
        """Specifies the type of date and time format that is used in the field"""
        return self.properties.get("DisplayFormat", None)

    @property
    def friendly_display_format(self):
        # type: () -> Optional[int]
        """Gets the type of friendly display format that is used in the field"""
        return self.properties.get("FriendlyDisplayFormat", None)

    @property
    def time_format(self):
        # type: () -> Optional[str]
        """Gets the time format that is used in the field"""
        return self.properties.get("TimeFormat", None)
