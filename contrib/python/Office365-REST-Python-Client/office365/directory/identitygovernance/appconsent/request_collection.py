from office365.directory.identitygovernance.appconsent.request import AppConsentRequest
from office365.entity_collection import EntityCollection
from office365.runtime.queries.function import FunctionQuery


class AppConsentRequestCollection(EntityCollection[AppConsentRequest]):
    """AppConsentRequest's collection"""

    def __init__(self, context, resource_path=None):
        super(AppConsentRequestCollection, self).__init__(
            context, AppConsentRequest, resource_path
        )

    def filter_by_current_user(self, on):
        """
        Retrieve a collection of appConsentRequest objects for which the current user is the reviewer and the status
        of the userConsentRequest for accessing the specified app is InProgress.
        :param str on:Filter to query appConsentRequest objects for which the current user is a reviewer.
           Allowed value is reviewer. Required.
        """
        return_type = AppConsentRequestCollection(self.context)
        params = {"on": on}
        qry = FunctionQuery(self, "filterByCurrentUser", params, return_type)
        self.context.add_query(qry)
        return return_type
