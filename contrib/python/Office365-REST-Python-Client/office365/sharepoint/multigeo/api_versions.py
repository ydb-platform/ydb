from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity


class MultiGeoApiVersions(Entity):
    """"""

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.Online.SharePoint.MultiGeo.Service.MultiGeoApiVersions"
            )
        super(MultiGeoApiVersions, self).__init__(context, resource_path)

    @property
    def supported_versions(self):
        return self.properties.get("SupportedVersions", StringCollection())

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MultiGeo.Service.MultiGeoApiVersions"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"SupportedVersions": self.supported_versions}
            default_value = property_mapping.get(name, None)
        return super(MultiGeoApiVersions, self).get_property(name, default_value)
