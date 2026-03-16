class BaseType:
    """Specifies the base type for a list."""

    None_ = -1

    GenericList = 0
    """Specifies a base type for lists that do not correspond to another base type in this enumeration."""

    DocumentLibrary = 1
    """Specifies a base type for a document library."""

    Unused = 2
    """Reserved. MUST NOT be used."""

    DiscussionBoard = 3
    """Specifies a base type that SHOULD NOT be used, but MAY<2> be used for a discussion board."""

    Survey = 4
    """Enumeration whose values specify a base type for a survey list."""

    Issue = 5
    """Specifies a base type for an issue tracking list."""
