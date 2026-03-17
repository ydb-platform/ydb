from office365.runtime.client_value import ClientValue


class TimeZoneInformation(ClientValue):
    """Provides information used to define a time zone."""

    def __init__(self, bias=None, standard_bias=None, daylight_bias=None):
        """

        :param int bias: Gets the bias in the number of minutes that the time zone differs from
            Coordinated Universal Time (UTC).
        :param daylight_bias: Gets the bias in the number of minutes that daylight time for the time zone
            differs from Coordinated Universal Time (UTC).
        :param standard_bias: Gets the bias in the number of minutes that standard time for the time zone differs
             from coordinated universal time (UTC).
        """
        super(TimeZoneInformation, self).__init__()
        self.Bias = bias
        self.DaylightBias = daylight_bias
        self.StandardBias = standard_bias
