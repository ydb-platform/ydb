from office365.directory.identitygovernance.userconsent.request import (
    UserConsentRequest,
)
from office365.entity_collection import EntityCollection
from office365.runtime.queries.function import FunctionQuery


class UserConsentRequestCollection(EntityCollection[UserConsentRequest]):
    """AppConsentRequest's collection"""

    def __init__(self, context, resource_path=None):
        super(UserConsentRequestCollection, self).__init__(
            context, UserConsentRequest, resource_path
        )

    def filter_by_current_user(self, on):
        """Retrieve a collection of userConsentRequest objects for accessing a specified app, for which the current
        user is the reviewer."""
        return_type = UserConsentRequestCollection(self.context, self.resource_path)
        params = {"on": on}
        qry = FunctionQuery(self, "filterByCurrentUser", params, return_type)
        self.context.add_query(qry)
        return return_type
