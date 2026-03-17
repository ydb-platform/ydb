from office365.runtime.client_value import ClientValue


class DlpClassificationResult(ClientValue):
    @property
    def entity_type_name(self):
        return "SP.CompliancePolicy.DlpClassificationResult"
