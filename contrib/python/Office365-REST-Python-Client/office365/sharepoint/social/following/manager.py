from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.social.actor import SocialActor


class SocialFollowingManager(Entity):
    """The SocialFollowingManager class provides properties and methods for managing a user's list of followed actors.
    Actors can be users, documents, sites, and tags."""

    def __init__(self, context):
        super(SocialFollowingManager, self).__init__(
            context, ResourcePath("SP.Social.SocialFollowingManager")
        )

    def get_followers(self):
        """
        The GetFollowers method returns the users who are followers of the current user.
        """
        return_type = ClientResult(self.context, ClientValueCollection(SocialActor))
        qry = ServiceOperationQuery(self, "GetFollowers", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_suggestions(self):
        """
        The GetSuggestions method returns a list of actors that are suggestions for the current user to follow.
        """
        return_type = ClientResult(self.context, ClientValueCollection(SocialActor))
        qry = ServiceOperationQuery(
            self, "GetSuggestions", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
