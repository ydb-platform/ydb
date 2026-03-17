from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.search.query.sort.sort import Sort


class SortCollection(Entity):
    """Contains information about how to sort the search results."""

    def add(self, property_name, direction):
        """
        Adds a new element of type Microsoft.SharePoint.Client.Search.Query.Sort to the collection and returns
        a reference to the added Microsoft.SharePoint.Client.Search.Query.Sort.

        :param str property_name: f direction is equal to SortDirection.Ascending or SortDirection.Descending, then
            this element specifies the name of the managed property to sort the search results on
        :param int direction: The direction in which to sort on the property specified in the strProperty element.
            MUST be a SortDirection data type as specified in section 3.1.4.7.4.4. If the direction element is
            not specified, the protocol server MUST use SortDirection.Ascending as the default. If direction is equal
            to SortDirection.FQLFormula, then the strProperty MUST specify the formula that MUST be used for sorting
            the search results, as specified in section 3.1.4.7.4.4. If QueryProperties.ResultsProvider
            (as specified in section 2.2.4.11) is SearchServer.SharepointSearch and direction is set to
            SortDirection.FQLFormula, the value of strProperty MUST be ignored by the protocol server.
        """
        payload = {"strProperty": property_name, "direction": direction}
        qry = ServiceOperationQuery(self, "Add", None, payload)
        self.context.add_query(qry)
        return self

    def clear(self):
        """Deletes all the elements in the collection."""
        qry = ServiceOperationQuery(self, "Clear")
        self.context.add_query(qry)
        return self

    @property
    def items(self):
        return self.properties.get("Items", ClientValueCollection(Sort))

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.SortCollection"
