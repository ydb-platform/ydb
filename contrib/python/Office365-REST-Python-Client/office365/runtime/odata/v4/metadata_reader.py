from office365.runtime.odata.reader import ODataReader


class ODataV4Reader(ODataReader):
    """OData v4 reader"""

    _options = None

    def __init__(self, metadata_path):
        super(ODataV4Reader, self).__init__(
            metadata_path,
            {
                "xmlns": "http://docs.oasis-open.org/odata/ns/edm",
                "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
            },
        )
