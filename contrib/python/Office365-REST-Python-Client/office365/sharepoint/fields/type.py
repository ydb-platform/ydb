class FieldType:
    def __init__(self):
        """Specifies the type of the field (2), as specified in [MS-WSSTS] section 2.3.1."""
        pass

    Invalid = 0
    """It MUST NOT be used."""

    Integer = 1
    """Specifies that the field (2) contains an integer value."""

    Text = 2
    """Specifies that the field (2) contains a single line of text."""

    Note = 3
    """Specifies that the field (2) contains multiple lines of text."""

    DateTime = 4
    """Specifies that the field (2) contains a date and time value or a date-only value."""

    Counter = 5
    """Specifies that the field (2) contains a monotonically increasing integer."""

    Choice = 6
    """Specifies that the field (2) contains a single value from a set of specified values."""

    Lookup = 7
    """Specifies that the field (2) is a lookup field."""

    Boolean = 8
    """Specifies that the field (2) contains a Boolean value."""

    Number = 9
    """Specifies that the field (2) contains a floating-point number value."""

    Currency = 10
    """Specifies that the field (2) contains a currency value."""

    URL = 11
    """Specifies that the field (2) contains a URI and an optional description of the URI."""

    Computed = 12
    """Specifies that the field (2) is a computed field."""

    Threading = 13
    """Specifies that the field (2) indicates the thread for a discussion item in a threaded view of
    a discussion board."""

    Guid = 14
    """Specifies that the field (2) contains a GUID value."""

    MultiChoice = 15
    """Specifies that the field (2) contains one or more values from a set of specified values."""

    GridChoice = 16
    """Specifies that the field (2) contains rating scale values for a survey list."""

    Calculated = 17
    """Specifies that the field (2) is a calculated field."""

    File = 18
    """Specifies that the field (2) contains the leaf name of a document as a value"""

    Attachments = 19
    """Specifies that the field (2) indicates whether the list item has attachments."""

    User = 20
    """Specifies that the field (2) contains one or more users and groups as values."""

    Recurrence = 21
    """Specifies that the field (2) indicates whether a meeting in a calendar list recurs."""

    CrossProjectLink = 22
    """Specifies that the field (2) contains a link between projects in a Meeting Workspace site."""

    ModStat = 23
    """Specifies that the field (2) indicates moderation status."""

    Error = 24
    """Specifies that the type of the field (2) was set to an invalid value."""

    ContentTypeId = 25

    PageSeparator = 26

    ThreadIndex = 27

    WorkflowStatus = 28

    AllDayEvent = 29

    WorkflowEventType = 30

    Geolocation = 31

    OutcomeChoice = 32

    MaxItems = 33
