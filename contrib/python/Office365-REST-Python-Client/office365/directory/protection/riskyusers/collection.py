from office365.directory.protection.riskyusers.risky_user import RiskyUser
from office365.entity_collection import EntityCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection


class RiskyUserCollection(EntityCollection[RiskyUser]):
    """RiskyUser's collection"""

    def __init__(self, context, resource_path=None):
        super(RiskyUserCollection, self).__init__(context, RiskyUser, resource_path)

    def confirm_compromised(self, user_ids=None):
        """Confirm one or more riskyUser objects as compromised. This action sets the targeted user's risk level
        to high.

        :param list[str] user_ids: Specify the risky user IDs to dismiss in the request body.
        """
        payload = {"userIds": StringCollection(user_ids)}
        qry = ServiceOperationQuery(self, "confirmCompromised", None, payload)
        self.context.add_query(qry)
        return self

    def dismiss(self, user_ids=None):
        """Dismiss the risk of one or more riskyUser objects. This action sets the targeted user's risk level to none.

        :param list[str] user_ids: Specify the risky user IDs to dismiss in the request body.
        """
        payload = {"userIds": StringCollection(user_ids)}
        qry = ServiceOperationQuery(self, "dismiss", None, payload)
        self.context.add_query(qry)
        return self
