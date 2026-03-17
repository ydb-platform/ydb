from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.webhooks.subscription import Subscription
from office365.sharepoint.webhooks.subscription_information import (
    SubscriptionInformation,
)


class SubscriptionCollection(EntityCollection[Subscription]):
    """Represents a collection of Subscription (WebHook) resources."""

    def __init__(self, context, resource_path=None, parent=None):
        super(SubscriptionCollection, self).__init__(
            context, Subscription, resource_path, parent
        )

    def get_by_id(self, _id):
        """Gets the subscription with the specified ID."""
        return Subscription(
            self.context, ServiceOperationPath("getById", [_id], self.resource_path)
        )

    def add(self, parameters):
        """
        :param SubscriptionInformation or str parameters: Subscription information object or notification string
        """
        return_type = Subscription(self.context)
        self.add_child(return_type)

        def _create_and_add_query(information):
            """
            :type information: SubscriptionInformation
            """
            payload = {"parameters": information}
            qry = ServiceOperationQuery(self, "Add", None, payload, None, return_type)
            self.context.add_query(qry)

        if isinstance(parameters, SubscriptionInformation):
            _create_and_add_query(parameters)
        else:

            def _parent_loaded():
                _create_and_add_query(
                    SubscriptionInformation(parameters, self._parent.properties["Id"])
                )

            self._parent.ensure_property("Id", _parent_loaded)
        return return_type

    def remove(self, subscription_id):
        """Removes the subscription with the specified subscriptionId from the collection.

        :param str subscription_id: The ID of the subscription.
        """
        payload = {
            "subscriptionId": subscription_id,
        }
        qry = ServiceOperationQuery(self, "Remove", payload, None, None, None)
        self.context.add_query(qry)
        return self
