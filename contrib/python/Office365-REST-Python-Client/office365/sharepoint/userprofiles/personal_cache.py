from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class PersonalCache(Entity):
    """Per user cache of key/value pairs organized by folders. Personal cache MAY be used for optimizing initial
    load performance of the protocol client, if obtaining initial set of data from personal cache is faster that
    requesting the data from the server."""

    def __init__(self, context):
        super(PersonalCache, self).__init__(
            context, ResourcePath("SP.UserProfiles.PersonalCache")
        )

    def dispose(self):
        """ """
        qry = ServiceOperationQuery(self, "Dispose")
        self.context.add_query(qry)
        return self

    @property
    def cache_name(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("CacheName", None)

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.PersonalCache"
