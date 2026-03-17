from office365.runtime.client_value import ClientValue


class PendingReviewItemsStatistics(ClientValue):
    def __init__(self, label_id=None, label_name=None):
        self.LabelId = label_id
        self.LabelName = label_name

    @property
    def entity_type_name(self):
        return "SP.CompliancePolicy.PendingReviewItemsStatistics"
