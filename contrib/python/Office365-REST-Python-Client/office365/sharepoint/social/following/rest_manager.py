from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.social.actor import SocialActor


class SocialRestFollowingManager(Entity):
    """Provides methods for managing a user's list of followed actors (users, documents, sites, and tags)."""

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath("SP.Social.SocialRestFollowingManager")
        super(SocialRestFollowingManager, self).__init__(context, resource_path)

    def followers(self):
        """The Followers method retrieves the current user's list of followers. For details on the SocialActor type,
        see section 3.1.5.3."""
        return_type = ClientResult(self.context, ClientValueCollection(SocialActor))
        qry = ServiceOperationQuery(self, "Followers", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def my(self):
        """The My method gets a SocialRestActor object that represents the current user. See section 3.1.5.35 for
        details on the SocialRestActor type."""
        return SocialRestFollowingManager(
            self.context, ResourcePath("My", self.resource_path)
        )

    @property
    def entity_type_name(self):
        return "SP.Social.SocialRestFollowingManager"
