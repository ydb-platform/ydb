class ChangeType:
    """Enumeration of the possible types of changes."""

    def __init__(self):
        pass

    NoChange = 0
    """Indicates that no change has taken place."""

    Add = 1
    """Specifies that an object has been added within the scope of a list, site, site collection, or content database"""

    Update = 2
    """Specifies that an object has been modified within the scope of a list, site, site collection,
    or content database."""

    DeleteObject = 3
    """
    Specifies that an object has been deleted within the scope of a list, site, site collection, or content database
    """

    Rename = 4
    """The leaf in a URL has been renamed."""

    MoveAway = 5
    """Specifies that a non-leaf segment within a URL has been renamed. The object was moved away from the
    location within the site specified by the change."""

    MoveInto = 6
    """Specifies that a non-leaf segment within a URL has been renamed. The object was moved into the location within
    the site specified by the change."""

    Restore = 7
    """Specifies that an object (1) has restored from a backup or from the Recycle Bin"""

    RoleAdd = 8
    """Specifies that a role definition has been added."""

    RoleDelete = 9
    """Specifies that a role definition has been deleted."""

    RoleUpdate = 10
    """Specifies that a role definition has been updated."""

    AssignmentAdd = 11
    """Specifies that a user has been given permissions to a list. The list MUST have different permissions from
    its parent."""

    AssignmentDelete = 12
    """Specifies that a user has lost permissions to a list. The list MUST have different permissions from its parent"""

    MemberAdd = 13
    """Specifies that a user has been added to a group."""

    MemberDelete = 14
    """Specifies that a user has been removed from a group."""

    SystemUpdate = 15
    """Specifies that a change has been made to an item by using the protocol server method."""

    Navigation = 16
    """Specifies that a change in the navigation structure of a site collection has been made."""

    ScopeAdd = 17
    """Specifies that a change in permissions scope has been made to break inheritance from the parent of an object """

    ScopeDelete = 18
    """Specifies that a change in permissions scope has been made to revert back to inheriting permissions from
    the parent of an object"""

    ListContentTypeAdd = 19
    """Specifies that a list content type has been added."""

    ListContentTypeDelete = 20
    """Specifies that a list content type has been deleted."""

    Dirty = 21
    """Specifies that this item has a pending modification due to an operation on another item."""

    Activity = 22
    """Specifies that an activity change as specified in section 3.2.5.462 has been made to the object """
