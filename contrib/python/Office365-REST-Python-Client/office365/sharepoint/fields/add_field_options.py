class AddFieldOptions:
    """Specifies the control settings while adding a field."""

    def __init__(self):
        pass

    DefaultValue = 0
    """Same as AddToDefaultContentType."""

    AddToDefaultContentType = 1
    """Specifies that a new field added to the list MUST also be added to the default content type in the site
    collection."""

    AddToNoContentType = 2
    """Specifies that a new field MUST NOT be added to any other content type."""

    AddToAllContentTypes = 4
    """Specifies that a new field that is added to the specified list MUST also be added to all content types in the
    site collection."""

    AddFieldInternalNameHint = 8
    """Specifies adding an internal field name hint for the purpose of avoiding possible database locking or field
    renaming operations."""

    AddFieldToDefaultView = 16
    """Specifies that a new field that is added to the specified list MUST also be added to the default list view."""

    AddFieldCheckDisplayName = 32
    """Specifies to confirm that no other field has the same display name."""
