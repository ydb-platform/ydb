from office365.directory.users.activities.activity import UserActivity
from office365.entity_collection import EntityCollection
from office365.runtime.queries.function import FunctionQuery


class UserActivityCollection(EntityCollection[UserActivity]):
    """User activities collection"""

    def __init__(self, context, resource_path=None):
        super(UserActivityCollection, self).__init__(
            context, UserActivity, resource_path
        )

    def recent(self):
        """
        List a set of items that have been recently used by the signed in user.
        This collection includes items that are in the user's drive as well as items
        they have access to from other drives.
        """
        return_type = UserActivityCollection(self.context, self.resource_path)
        qry = FunctionQuery(self, "recent", None, return_type)
        self.context.add_query(qry)
        return return_type
