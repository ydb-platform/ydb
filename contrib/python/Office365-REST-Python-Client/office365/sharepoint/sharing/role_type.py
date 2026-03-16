class RoleType:
    """Specifies the types of role definitions that are available for users and groups."""

    None_ = 0
    """The role definition has no rights on the site (2)."""

    Guest = 1
    """The role definition has limited right to view pages and specific page elements.
    This role is used to give users access to a particular page, list, or item in a list, without granting rights
    to view the entire site (2). Users cannot be added explicitly to the guest role; users who are given access to
    lists or document libraries by using permissions for a specific list are added automatically to the guest role.
    he guest role cannot be customized or deleted."""

    Reader = 2
    """
    The role definition has a right to view items, personalize Web Parts, use alerts, and create a top-level site.
    A reader can only read a site (2); the reader cannot add content. When a reader creates a site (2), the reader
    becomes the site (2) owner and a member of the administrator role for the new site (2).
    This does not affect the user's role membership for any other site (2).
    Rights included are CreateSSCSite, ViewListItems, and ViewPages.
    """

    Contributor = 3
    """
    The role definition has reader rights, and a right to add items, edit items, delete items, manage list permissions,
     manage personal views, personalize Web Part Pages, and browse directories. Includes all rights in the reader role,
     and AddDelPrivateWebParts, AddListItems, BrowseDirectories, CreatePersonalGroups, DeleteListItems, EditListItems,
     ManagePersonalViews, and UpdatePersonalWebParts roles. Contributors cannot create new lists or document libraries,
     but they can add content to existing lists and document libraries.
    """

    WebDesigner = 4
    """Has Contributor rights, plus rights to cancel check-out, delete items, manage lists, add and customize pages,
    define and apply themes and borders, and link style sheets. Includes all rights in the Contributor role, plus the
    following: AddAndCustomizePages, ApplyStyleSheets, ApplyThemeAndBorder, CancelCheckout, ManageLists. WebDesigners
    can modify the structure of the site and create new lists or document libraries. The value = 4."""

    Administrator = 5
    """Has all rights from other roles, plus rights to manage roles and view usage analysis data.
    Includes all rights in the WebDesigner role, plus the following: ManageListPermissions, ManageRoles, ManageSubwebs,
    ViewUsageData. The Administrator role cannot be customized or deleted, and must always contain at least one member.
    Members of the Administrator role always have access to, or can grant themselves access to, any item in the
    Web site. The value = 5."""

    Editor = 6
    """
        The role definition has reader rights, plus rights to Review items. Inclues all rights in the Reader role, plus
        the following: ReviewListItems. Reviewers cannot create new lists and document libraries, but they can add
        comments to existing list items or documents.
        """

    Reviewer = 7
    """The role definition is a special reader role definition with restricted rights, it has a right to view
        the content of the file but cannot download the file directly. Includes all rights in the Guest role, plus
        the following: ViewPages, ViewListItems and ViewItemsRequiresOpen."""

    RestrictedReader = 8
    """"""

    System = 255
    """For SharePoint internal use only. System roles can not be deleted, nor modified."""
