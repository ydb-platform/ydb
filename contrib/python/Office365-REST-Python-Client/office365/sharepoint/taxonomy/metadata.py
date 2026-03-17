from office365.runtime.client_value import ClientValue


class TaxonomyMetadata(ClientValue):
    def __init__(self, anchor_id=None):
        """
        :param str anchor_id:
        """
        self.anchorId = anchor_id

    @property
    def entity_type_name(self):
        return "SP.Publishing.TaxonomyMetadata"
