from office365.runtime.client_value import ClientValue


class TimeZoneBase(ClientValue):
    """The basic representation of a time zone."""

    def __init__(self, name=None):
        """
        :param str name: The name of a time zone. It can be a standard time zone name such as
            "Hawaii-Aleutian Standard Time", or "Customized Time Zone" for a custom time zone.
        """
        self.name = name

    def __repr__(self):
        return self.name
