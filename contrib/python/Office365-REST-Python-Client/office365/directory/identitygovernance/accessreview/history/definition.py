from office365.directory.identitygovernance.accessreview.scope import AccessReviewScope
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection


class AccessReviewHistoryDefinition(Entity):
    """
    Represents a collection of access review historical data and the scopes used to collect that data.

    An accessReviewHistoryDefinition contains a list of accessReviewHistoryInstance objects.
    Each recurrence of the history definition creates an instance. In the case of a one-time history definition,
    only one instance is created.
    """

    @property
    def scopes(self):
        """
        Used to scope what reviews are included in the fetched history data. Fetches reviews whose scope matches with
        this provided scope. Required.
        """
        return self.properties.get("scopes", ClientValueCollection(AccessReviewScope))
