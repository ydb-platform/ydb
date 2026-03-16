from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class ConnectionSettings(Entity):
    """The ConnectionSettings contains information about an endpoint (4) that can be used to connect to it."""

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.BusinessData.Infrastructure.SecureStore.ConnectionSettings"
            )
        super(ConnectionSettings, self).__init__(context, resource_path)

    @property
    def authentication_mode(self):
        # type: () -> Optional[str]
        """The authentication mode used by the endpoint"""
        return self.properties.get("AuthenticationMode", None)

    @property
    def parent_name(self):
        # type: () -> Optional[str]
        """The unique name used to identify the parent of the endpoint"""
        return self.properties.get("parentName", None)

    @property
    def entity_type_name(self):
        return "Microsoft.BusinessData.Infrastructure.SecureStore.ConnectionSettings"
