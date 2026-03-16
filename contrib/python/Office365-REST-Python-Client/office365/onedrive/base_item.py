from datetime import datetime
from typing import Optional

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.onedrive.listitems.item_reference import ItemReference
from office365.runtime.paths.resource_path import ResourcePath


class BaseItem(Entity):
    """The baseItem resource is an abstract resource that contains a auth set of properties shared among several
    other resources types"""

    @property
    def etag(self):
        # type: () -> Optional[str]
        """ETag for the item."""
        return self.properties.get("eTag", None)

    @property
    def created_by(self):
        # type: () -> IdentitySet
        """Identity of the user, device, or application which created the item."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def created_by_user(self):
        """Identity of the user who created the item"""
        from office365.directory.users.user import User

        return self.properties.get(
            "createdByUser",
            User(self.context, ResourcePath("createdByUser", self.resource_path)),
        )

    @property
    def last_modified_by(self):
        # type: () -> IdentitySet
        """Identity of the user, device, and application which last modified the item."""
        return self.properties.get("lastModifiedBy", IdentitySet())

    @property
    def last_modified_by_user(self):
        """Identity of the user who last modified the item."""
        from office365.directory.users.user import User

        return self.properties.get(
            "lastModifiedByUser",
            User(self.context, ResourcePath("lastModifiedByUser", self.resource_path)),
        )

    @property
    def created_datetime(self):
        # type: () -> Optional[datetime]
        """Gets date and time of item creation."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def last_modified_datetime(self):
        # type: () -> Optional[datetime]
        """Gets date and time the item was last modified."""
        return self.properties.get("lastModifiedDateTime", datetime.min)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Gets the name of the item."""
        return self.properties.get("name", None)

    @name.setter
    def name(self, value):
        # type: (str) -> None
        """Sets the name of the item."""
        self.set_property("name", value)

    @property
    def description(self):
        # type: () -> Optional[str]
        """Provides a user-visible description of the item."""
        return self.properties.get("description", None)

    @description.setter
    def description(self, value):
        # type: (str) -> None
        self.set_property("description", value)

    @property
    def web_url(self):
        # type: () -> Optional[str]
        """URL that displays the resource in the browser"""
        return self.properties.get("webUrl", None)

    @property
    def parent_reference(self):
        # type: () -> ItemReference
        """Parent information, if the item has a parent."""
        return self.properties.setdefault("parentReference", ItemReference())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
                "createdByUser": self.created_by_user,
                "createdDateTime": self.created_datetime,
                "lastModifiedDateTime": self.last_modified_datetime,
                "lastModifiedBy": self.last_modified_by,
                "lastModifiedByUser": self.last_modified_by_user,
                "parentReference": self.parent_reference,
            }
            default_value = property_mapping.get(name, None)
        return super(BaseItem, self).get_property(name, default_value)
