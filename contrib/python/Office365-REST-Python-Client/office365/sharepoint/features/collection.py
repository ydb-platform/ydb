from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.features.feature import Feature


class FeatureCollection(EntityCollection[Feature]):
    """Represents a collection of Feature resources."""

    def __init__(self, context, resource_path=None, parent=None):
        super(FeatureCollection, self).__init__(context, Feature, resource_path, parent)

    def add(self, feature_id, force, featdef_scope, verify_if_activated=False):
        # type: (str, bool, int, bool) -> Feature
        """
        Adds the feature to the collection of activated features and returns the added feature.

        :param str feature_id: The feature identifier of the feature to be added.
        :param bool force: Specifies whether to continue with the operation even if there are errors.
        :param int featdef_scope: The feature scope for this feature.
        :param bool verify_if_activated: Verify if activated first to avoid System.Data.DuplicateNameException exception
        """
        return_type = Feature(self.context)
        self.add_child(return_type)

        def _create_query():
            payload = {
                "featureId": feature_id,
                "force": force,
                "featdefScope": featdef_scope,
            }
            return ServiceOperationQuery(self, "Add", None, payload, None, return_type)

        def _create_if_not_activated(f):
            # type: (Feature) -> None
            if not f.properties:
                self.context.add_query(_create_query())

        if verify_if_activated:
            self.get_by_id(feature_id).get().after_execute(_create_if_not_activated)
        else:
            self.context.add_query(_create_query())

        return return_type

    def get_by_id(self, feature_id):
        # type: (str) -> Feature
        """Returns the feature for the given feature identifier. Returns NULL if no feature is available for the given
            feature identifier.

        :param str feature_id:  The feature identifier of the feature to be returned.
        """
        return Feature(
            self.context,
            ServiceOperationPath("GetById", [feature_id], self.resource_path),
        )
