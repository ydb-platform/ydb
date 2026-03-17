from office365.directory.identitygovernance.accessreview.history.definition import (
    AccessReviewHistoryDefinition,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class AccessReviewSet(Entity):
    """
    Container for the base resources that expose the access reviews API and features. Currently exposes only the
    accessReviewScheduleDefinition relationship.
    """

    @property
    def history_definitions(self):
        """
        Represents a collection of access review history data and the scopes used to collect that data
        """
        return self.properties.get(
            "historyDefinitions",
            EntityCollection(
                self.context,
                AccessReviewHistoryDefinition,
                ResourcePath("historyDefinitions", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"historyDefinitions": self.history_definitions}
            default_value = property_mapping.get(name, None)
        return super(AccessReviewSet, self).get_property(name, default_value)
