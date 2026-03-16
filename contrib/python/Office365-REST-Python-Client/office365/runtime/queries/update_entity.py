from office365.runtime.client_object import ClientObject
from office365.runtime.queries.client_query import ClientQuery


class UpdateEntityQuery(ClientQuery):
    def __init__(self, update_type):
        # type: (ClientObject) -> None
        """Update client object query"""
        super(UpdateEntityQuery, self).__init__(
            update_type.context, update_type, update_type
        )
