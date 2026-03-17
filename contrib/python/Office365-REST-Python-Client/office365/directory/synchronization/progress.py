from office365.runtime.client_value import ClientValue


class SynchronizationProgress(ClientValue):
    """Represents the progress of a synchronizationJob toward completion."""

    def __init__(
        self,
        completed_units=None,
        progress_observation_date_time=None,
        total_units=None,
        units=None,
    ):
        self.completedUnits = completed_units
        self.progressObservationDateTime = progress_observation_date_time
        self.totalUnits = total_units
        self.units = units
