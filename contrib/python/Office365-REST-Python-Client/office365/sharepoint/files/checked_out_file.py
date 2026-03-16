from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v3.entity import EntityPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.principal.users.user import User


class CheckedOutFile(Entity):
    """Represents a checked-out file in a document library or workspace."""

    def takeover_checkout(self):
        """Instructs the site that another user account is taking over control of a currently checked-out file."""
        qry = ServiceOperationQuery(self, "TakeOverCheckOut")
        self.context.add_query(qry)
        return self

    @property
    def checked_out_by_id(self):
        # type: () -> Optional[int]
        """Returns the user ID of the account used to check out the file."""
        return self.properties.get("CheckedOutById", None)

    @property
    def checked_out_by(self):
        """Returns the user name of the account used to check out the file."""
        return self.properties.get(
            "CheckedOutBy",
            User(self.context, ResourcePath("CheckedOutBy", self.resource_path)),
        )

    @property
    def property_ref_name(self):
        return "CheckedOutById"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"CheckedOutBy": self.checked_out_by}
            default_value = property_mapping.get(name, None)
        return super(CheckedOutFile, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(CheckedOutFile, self).set_property(name, value, persist_changes)
        # fallback: create a new resource path
        if name == "CheckedOutById":
            self._resource_path = EntityPath(
                value, self.parent_collection.resource_path
            )
        return self
