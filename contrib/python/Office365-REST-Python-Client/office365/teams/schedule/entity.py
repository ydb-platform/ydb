from office365.runtime.client_value import ClientValue


class ScheduleEntity(ClientValue):
    """"""

    def __init__(self, end_datetime=None):
        self.endDateTime = end_datetime
