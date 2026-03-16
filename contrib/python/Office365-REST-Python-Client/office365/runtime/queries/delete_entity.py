from typing import TYPE_CHECKING

from office365.runtime.queries.client_query import ClientQuery

if TYPE_CHECKING:
    from office365.runtime.client_object import ClientObject


class DeleteEntityQuery(ClientQuery):
    def __init__(self, delete_type):
        # type: (ClientObject) -> None
        """
        Delete entity query
        """
        super(DeleteEntityQuery, self).__init__(delete_type.context, delete_type)
