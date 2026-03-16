from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.context_condition import ContextCondition


class PromotedResultQueryRule(ClientValue):
    """
    This object contains properties that describe one promoted result for the tenant/Search
    Service Application or site collection.
    """

    def __init__(self, contact=None, context_conditions=None, creation_date=None):
        """
        :param str contact: This property contains the contact information for the promoted result.
        :param list[ContextCondition] context_conditions: This property contains the context condition for the promoted
            result.
        :param str creation_date: This property is the creation date for the promoted result.
        """
        self.Contact = contact
        self.ContextConditions = ClientValueCollection(
            ContextCondition, context_conditions
        )
        self.CreationDate = creation_date

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.PromotedResultQueryRule"
