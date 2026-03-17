class ODataV4MetadataLevel:
    Full = "full"
    """Indicates that the service MUST include all control information explicitly in the payload."""

    NoMetadata = "none"
    """Indicates that the service SHOULD omit control information other than odata.nextLink and odata.count"""

    Minimal = "minimal"
    """Indicates that the service SHOULD remove computable control information from the payload wherever possible.
    This is the default value for the odata.metadata parameter and will be assumed if no other value is specified in
    the Accept header or $format query option"""
