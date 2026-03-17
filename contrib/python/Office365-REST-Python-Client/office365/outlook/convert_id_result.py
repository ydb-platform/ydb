from office365.runtime.client_value import ClientValue


class ConvertIdResult(ClientValue):
    """The result of an ID format conversion performed by the translateExchangeIds function."""

    def __init__(self, source_id=None, target_id=None):
        """
        :param str source_id: The identifier that was converted. This value is the original, un-converted identifier.
        :param str target_id: The converted identifier. This value is not present if the conversion failed.
        """
        self.sourceId = source_id
        self.targetId = target_id
