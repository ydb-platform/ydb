from office365.runtime.client_value import ClientValue


class ActivityTimeFacet(ClientValue):
    """"""

    def __init__(self, last_recorded_time=None, observed_time=None, recorded_time=None):
        """
        :param str last_recorded_time:
        :param str observed_time:
        :param str recorded_time:
        """
        self.lastRecordedTime = last_recorded_time
        self.observedTime = observed_time
        self.recordedTime = recorded_time
