from office365.runtime.client_value import ClientValue


class TimeZoneInformation(ClientValue):
    """Represents a time zone. The supported format is Windows, and Internet Assigned Numbers Authority (IANA)
    time zone (also known as Olson time zone) format as well when the current known problem is fixed.
    """

    def __init__(self, alias=None, display_name=None):
        """
        :param str alias: An identifier for the time zone.
        :param str display_name: A display string that represents the time zone.
        """
        self.alias = alias
        self.displayName = display_name

    def __repr__(self):
        return self.displayName
