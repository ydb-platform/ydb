from office365.runtime.client_value import ClientValue


class ContextCondition(ClientValue):
    """
    This object contains properties that describe the context condition for the tenant.
    """

    def __init__(self, context_condition_type=None, source_id=None):
        """
        :param str context_condition_type: This property contains the context condition type for this context condition.
        :param str source_id: This property contains the source id for this context condition.
        """
        self.ContextConditionType = context_condition_type
        self.SourceId = source_id

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.ContextCondition"
