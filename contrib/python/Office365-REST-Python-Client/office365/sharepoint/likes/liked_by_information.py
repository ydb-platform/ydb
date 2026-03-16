from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.likes.user_entity import UserEntity


class LikedByInformation(Entity):
    """Represents the information about the set of users who liked the list item."""

    @property
    def like_count(self):
        # type: () -> Optional[int]
        """Number of users that have liked the item."""
        return self.properties.get("LikeCount", None)

    @property
    def is_liked_by_user(self):
        # type: () -> Optional[bool]
        """MUST be TRUE if the current user has liked the list item."""
        return self.properties.get("isLikedByUser", None)

    @property
    def liked_by(self):
        # type: () -> EntityCollection[UserEntity]
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

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "likedBy": self.liked_by,
            }
            default_value = property_mapping.get(name, None)
        return super(LikedByInformation, self).get_property(name, default_value)
