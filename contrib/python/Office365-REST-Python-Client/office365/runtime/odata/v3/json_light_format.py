from office365.runtime.odata.json_format import ODataJsonFormat
from office365.runtime.odata.v3.metadata_level import ODataV3MetadataLevel


class JsonLightFormat(ODataJsonFormat):
    """JSON Light format for SharePoint Online/One Drive for Business"""

    def __init__(self, metadata_level=ODataV3MetadataLevel.Verbose):
        super(JsonLightFormat, self).__init__(metadata_level)
        self.function = None

    @property
    def security(self):
        return "d"

    @property
    def collection(self):
        if self.metadata_level == ODataV3MetadataLevel.Verbose:
            return "results"
        else:
            return "value"

    @property
    def collection_next(self):
        """Property name for a reference to the next page of results"""
        return "__next"

    @property
    def collection_delta(self):
        """Property name for a reference to the next page of results"""
        return "__delta"

    @property
    def metadata_type(self):
        return "__metadata"

    @property
    def deferred_type(self):
        return "__deferred"

    @property
    def value_tag(self):
        return "__value"

    @property
    def etag(self):
        """The entity tag that represents the version of the object."""
        return "__etag"

    @property
    def media_type(self):
        return "application/json;odata={0}".format(self.metadata_level)

    @property
    def include_control_information(self):
        return (
            self.metadata_level == ODataV3MetadataLevel.Verbose
            or self.metadata_level == ODataV3MetadataLevel.MinimalMetadata
        )
