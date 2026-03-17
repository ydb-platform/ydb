from typing import Optional

from office365.sharepoint.entity import Entity


class ComponentContextInfo(Entity):
    """This class functions as a wrapper of the ContextInfo object. Reserved for internal use only."""

    @property
    def serialized_data(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("SerializedData", None)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Internal.ClientSideComponent.ComponentContextInfo"
