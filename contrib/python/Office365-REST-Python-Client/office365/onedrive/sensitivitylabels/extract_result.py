from office365.onedrive.sensitivitylabels.assignment import SensitivityLabelAssignment
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class ExtractSensitivityLabelsResult(ClientValue):
    """Represents the response format for the extractSensitivityLabels API."""

    def __init__(self, labels=None):
        """
        :param list[SensitivityLabelAssignment] labels: List of sensitivity labels assigned to a file.
        """
        self.labels = ClientValueCollection(SensitivityLabelAssignment, labels)
