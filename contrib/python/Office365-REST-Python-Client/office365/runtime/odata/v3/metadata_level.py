class ODataV3MetadataLevel:
    """The amount of metadata information to serialize in an OData response."""

    def __init__(self):
        pass

    Verbose = "verbose"
    """Indicates that the service MUST include all control information explicitly in the payload."""

    NoMetadata = "nometadata"
    """Indicates that the service SHOULD omit control information other than odata.nextLink and odata.count"""

    MinimalMetadata = "minimalmetadata"
    """Indicates that the service SHOULD remove computable control information from the payload wherever possible"""
