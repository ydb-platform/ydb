from office365.runtime.odata.json_format import ODataJsonFormat
from office365.runtime.odata.v4.metadata_level import ODataV4MetadataLevel


class V4JsonFormat(ODataJsonFormat):
    """JSON format (V4)"""

    def __init__(self, metadata_level=ODataV4MetadataLevel.Minimal):
        super(V4JsonFormat, self).__init__(metadata_level)
        """The IEEE754Compatible format parameter indicates that the service MUST serialize Edm.Int64 and
        Edm.Decimal numbers as strings."""
        self.IEEE754Compatible = False
        self.streaming = False

    @property
    def metadata_type(self):
        """The OData entity type in Microsoft Graph that describes the represented object."""
        return "@odata.type"

    @property
    def value_tag(self):
        """The OData entity type in Microsoft Graph that describes the represented object."""
        return "@odata.value"

    @property
    def collection(self):
        return "value"

    @property
    def collection_next(self):
        """
        Property name for a reference to the next page of results
        """
        return "@odata.nextLink"

    @property
    def collection_delta(self):
        """ """
        return "@odata.deltaLink"

    @property
    def etag(self):
        """The entity tag that represents the version of the object."""
        return "@odata.etag"

    @property
    def media_type(self):
        return "application/json;odata.metadata={0};odata.streaming={1};IEEE754Compatible={2}".format(
            self.metadata_level, self.streaming, self.IEEE754Compatible
        )

    @property
    def include_control_information(self):
        return (
            self.metadata_level == ODataV4MetadataLevel.Minimal
            or self.metadata_level == ODataV4MetadataLevel.Full
        )
