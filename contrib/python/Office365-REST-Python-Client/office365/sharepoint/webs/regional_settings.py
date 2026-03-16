from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.sites.language_collection import LanguageCollection
from office365.sharepoint.webs.time_zone import TimeZone, TimeZoneCollection


class RegionalSettings(Entity):
    """Represents regional settings that are used on the server that is running SharePoint Server."""

    @property
    def adjust_hijri_days(self):
        # type: () -> Optional[int]
        """Specifies the number of days to extend or reduce the current month in Hijri calendars on the site"""
        return self.properties.get("AdjustHijriDays", None)

    @property
    def alternate_calendar_type(self):
        # type: () -> Optional[int]
        """Gets an alternate calendar type that is used on the site"""
        return self.properties.get("AlternateCalendarType", None)

    @property
    def am(self):
        # type: () -> Optional[str]
        """Specifies the string that is used to represent time before midday on the site"""
        return self.properties.get("AM", None)

    @property
    def calendar_type(self):
        # type: () -> Optional[int]
        """Specifies the calendar type that SHOULD be used when processing date values on the site"""
        return self.properties.get("CalendarType", None)

    @property
    def collation(self):
        # type: () -> Optional[int]
        """
        Specifies the collation order of the site (2), which indicates an additional sorting order that SHOULD
        be processed by any back-end database server associated with the site (2). The collation method is an
        implementation-specific capability of the front-end Web server and back-end database server.
        """
        return self.properties.get("Collation", None)

    @property
    def collation_lcid(self):
        # type: () -> Optional[int]
        """Gets the language code identifier (LCID) for the collation rules that are used on the site"""
        return self.properties.get("CollationLCID", None)

    @property
    def date_format(self):
        # type: () -> Optional[int]
        """Gets the date format that is used on the server."""
        return self.properties.get("DateFormat", None)

    @property
    def date_separator(self):
        # type: () -> Optional[str]
        """Gets the separator that is used for dates on the server."""
        return self.properties.get("DateSeparator", None)

    @property
    def decimal_separator(self):
        # type: () -> Optional[str]
        """Gets the separator that is used for decimals on the server."""
        return self.properties.get("DecimalSeparator", None)

    @property
    def digit_grouping(self):
        # type: () -> Optional[str]
        """Gets the separator that is used in grouping digits."""
        return self.properties.get("DigitGrouping", None)

    @property
    def first_day_of_week(self):
        # type: () -> Optional[int]
        """Specifies the first day of the week used in calendars on the server.
        The possible values are specified in the following table:

        - 0 Sunday
        - 1 Monday
        - 2 Tuesday
        - 3 Wednesday
        - 4 Thursday
        - 5 Friday
        - 6 Saturday
        """
        return self.properties.get("FirstDayOfWeek", None)

    @property
    def first_week_of_year(self):
        # type: () -> Optional[int]
        """Specifies the first week of the year used in calendars on the server.
        Specifies which week of a year is considered the first week."""
        return self.properties.get("FirstWeekOfYear", None)

    @property
    def is_east_asia(self):
        # type: () -> Optional[bool]
        """Returns "true" if the web locale is an East Asia locale; "false" otherwise."""
        return self.properties.get("IsEastAsia", None)

    @property
    def is_right_to_left(self):
        # type: () -> Optional[bool]
        """Specifies whether the site uses a right-to-left language. If it uses a right-to-left language,
        this value MUST be "true". Otherwise, this value MUST be "false"."""
        return self.properties.get("IsRightToLeft", None)

    @property
    def is_ui_right_to_left(self):
        # type: () -> Optional[bool]
        """Specifies whether the UI of the site (2) uses a right-to-left language. If it uses a right-to-left language,
        this value MUST be "true". Otherwise, this value MUST be "false"."""
        return self.properties.get("IsUIRightToLeft", None)

    @property
    def list_separator(self):
        # type: () -> Optional[str]
        """Specifies the separator that is used for lists on the server. For example, if the separator is ",",
        items in a list will appear as "1,2,3,4"."""
        return self.properties.get("ListSeparator", None)

    @property
    def locale_id(self):
        # type: () -> Optional[int]
        """Gets the locale identifier in use on the server."""
        return self.properties.get("LocaleId", None)

    @property
    def negative_sign(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("NegativeSign", None)

    @property
    def neg_number_mode(self):
        # type: () -> Optional[int]
        """Specifies the negative number mode that is used for displaying negative numbers in calculated fields
        based on the locale of the server."""
        return self.properties.get("NegNumberMode", None)

    @property
    def pm(self):
        # type: () -> Optional[str]
        """Specifies the string that is used to represent time after noon on the site"""
        return self.properties.get("PM", None)

    @property
    def positive_sign(self):
        # type: () -> Optional[str]
        """Specifies the symbol that is used to represent a positive number on the site"""
        return self.properties.get("PositiveSign", None)

    @property
    def show_weeks(self):
        # type: () -> Optional[bool]
        """Specifies whether to display the week number in day or week views of a calendar."""
        return self.properties.get("ShowWeeks", None)

    @property
    def thousand_separator(self):
        # type: () -> Optional[str]
        """Specifies the symbol that is used to separate thousands for numbers on the site. For example,
        if the symbol is "," the number 123456789 would appear as "123,456,789"."""
        return self.properties.get("ThousandSeparator", None)

    @property
    def work_days(self):
        # type: () -> Optional[int]
        """Gets a number that represents the work days of Web site calendars."""
        return self.properties.get("WorkDays", None)

    @property
    def time_zone(self):
        """Gets the time zone that is used on the server."""
        return self.properties.get(
            "TimeZone",
            TimeZone(self.context, ResourcePath("TimeZone", self.resource_path)),
        )

    @property
    def time_zones(self):
        """Gets the collection of time zones used in a server farm."""
        return self.properties.get(
            "TimeZones",
            TimeZoneCollection(
                self.context, ResourcePath("TimeZones", self.resource_path)
            ),
        )

    @property
    def installed_languages(self):
        return self.properties.get(
            "InstalledLanguages",
            LanguageCollection(
                self.context, ResourcePath("InstalledLanguages", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "TimeZones": self.time_zones,
                "TimeZone": self.time_zone,
                "InstalledLanguages": self.installed_languages,
            }
            default_value = property_mapping.get(name, None)
        return super(RegionalSettings, self).get_property(name, default_value)
