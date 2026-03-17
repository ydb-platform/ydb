from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.likes.user_entity import UserEntity


class Comment(Entity):
    def like(self):
        """
        The Like method makes the current user a liker of the comment.
        """
        qry = ServiceOperationQuery(self, "Like")
        self.context.add_query(qry)
        return self

    def unlike(self):
        """
        The Unlike method removes the current user from the list of likers for the comment.
        """
        qry = ServiceOperationQuery(self, "Unlike")
        self.context.add_query(qry)
        return self

    @property
    def liked_by(self):
        """
        List of like entries corresponding to individual likes. MUST NOT contain more than one entry
        for the same user in the set.
        """
        return self.properties.get(
            "likedBy",
            EntityCollection(
                self.context, UserEntity, ResourcePath("likedBy", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Comments.comment"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "likedBy": self.liked_by,
            }
            default_value = property_mapping.get(name, None)
        return super(Comment, self).get_property(name, default_value)
