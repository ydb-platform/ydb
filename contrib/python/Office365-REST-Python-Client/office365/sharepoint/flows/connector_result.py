from typing import Optional

from office365.sharepoint.entity import Entity


class ConnectorResult(Entity):
    """ """

    @property
    def context_data(self):
        # type: () -> Optional[str]
        return self.properties.get("ContextData", None)

    @property
    def value(self):
        # type: () -> Optional[str]
        return self.properties.get("Value", None)
