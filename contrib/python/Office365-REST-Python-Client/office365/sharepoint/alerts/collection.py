from office365.runtime.client_result import ClientResult
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.alerts.alert import Alert
from office365.sharepoint.entity_collection import EntityCollection


class AlertCollection(EntityCollection[Alert]):
    """Content Type resource collection"""

    def __init__(self, context, resource_path=None):
        super(AlertCollection, self).__init__(context, Alert, resource_path)

    def add(self, parameters):
        """
        Add an SP.Alert to SP.AlertCollection based on the given alertCreationInformation

        :type parameters: office365.sharepoint.alerts.creation_information.AlertCreationInformation
        """
        return_type = Alert(self.context)
        self.add_child(return_type)
        qry = ServiceOperationQuery(
            self, "Add", None, parameters, "alertCreationInformation", return_type
        )
        self.context.add_query(qry)
        return return_type

    def contains(self, id_alert):
        # type: (str) -> ClientResult[bool]
        """
        Returns true if the given alert exists in the alert collection. False otherwise.

        :param str id_alert: The Id of the alert to search.
        """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "Contains", {"idAlert": id_alert}, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def delete_alert_at_index(self, index):
        """Deletes the alert based on the given index.

        :param int index:  A 32-bit integer that is greater than or equal to 0 and less than the count of the
              alert in the collection. Specifies the index of the alert in the collection.
        """
        qry = ServiceOperationQuery(
            self, "DeleteAlertAtIndex", {"index": index}, None, None, None
        )
        self.context.add_query(qry)
        return self

    def get_by_id(self, id_alert):
        """
        Gets an alert based on the Id.

        :param str id_alert: The Id of the alert to get.
        """
        return Alert(
            self.context,
            ServiceOperationPath("GetById", [id_alert], self.resource_path),
        )
