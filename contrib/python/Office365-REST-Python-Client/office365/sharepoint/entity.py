from typing import TYPE_CHECKING, Callable, List

from typing_extensions import Self

from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_object import ClientObject
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.v3.entity import EntityPath
from office365.runtime.queries.delete_entity import DeleteEntityQuery
from office365.runtime.queries.update_entity import UpdateEntityQuery

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class Entity(ClientObject):
    """SharePoint specific entity"""

    def execute_query_with_incremental_retry(self, max_retry=5):
        """Handles throttling requests."""
        self.context.execute_query_with_incremental_retry(max_retry)
        return self

    def execute_batch(self, items_per_batch=100, success_callback=None):
        # type: (int, Callable[[List[ClientObject|ClientResult]], None]) -> Self
        """Construct and submit to a server a batch request"""
        return self.context.execute_batch(items_per_batch, success_callback)

    def with_credentials(self, credentials):
        # type: (UserCredential|ClientCredential) -> Self
        """Initializes a client to acquire a token via user or client credentials"""
        self.context.with_credentials(credentials)
        return self

    def delete_object(self):
        """The recommended way to delete a SharePoint entity"""
        qry = DeleteEntityQuery(self)
        self.context.add_query(qry)
        self.remove_from_parent_collection()
        return self

    def update(self, *args):
        """The recommended way to update a SharePoint entity"""
        qry = UpdateEntityQuery(self)
        self.context.add_query(qry)
        return self

    @property
    def context(self):
        # type: () -> ClientContext
        return self._context

    @property
    def entity_type_name(self):
        if self._entity_type_name is None:
            self._entity_type_name = ".".join(["SP", type(self).__name__])
        return self._entity_type_name

    @property
    def property_ref_name(self):
        return "Id"

    def set_property(self, name, value, persist_changes=True):
        super(Entity, self).set_property(name, value, persist_changes)
        if name == self.property_ref_name:
            if self.resource_path is None:
                if self.parent_collection:
                    self._resource_path = EntityPath(
                        value, self.parent_collection.resource_path
                    )
                else:
                    pass
            else:
                self._resource_path.patch(value)
        return self
