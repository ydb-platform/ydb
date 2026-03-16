class PermissionKind:
    """Specifies permissions that are used to define user roles."""

    def __init__(self):
        pass

    EmptyMask = 0

    ViewListItems = 1
    """Allow viewing of list items in lists, documents in document libraries, and Web discussion comments."""

    AddListItems = 2
    """Allow addition of list items to lists, documents to document libraries, and Web discussion comments."""

    EditListItems = 3
    """Allow editing of list items in lists, documents in document libraries, Web discussion comments, and to customize
    Web Part pages in document libraries."""

    DeleteListItems = 4
    """Allow deletion of list items from lists, documents from document libraries, and Web discussion comments."""

    ApproveItems = 5
    """Allow approval of minor versions of a list item or document."""

    OpenItems = 6
    """Allow viewing the source of documents with server-side file handlers."""

    ViewVersions = 7
    """Allow viewing of past versions of a list item or document"""

    DeleteVersions = 8
    """Allow deletion of past versions of a list item or document."""

    CancelCheckout = 9
    """Allow discard or check in of a document that is checked out to another user."""

    ManagePersonalViews = 10
    """Allow creation, change, and deletion of personal views of lists."""

    ManageLists = 12
    """Allow creation and deletion of lists, addition or removal of fields to the schema of a list, and addition or
    removal of personal views of a list."""

    ViewFormPages = 13
    """Allow viewing of forms, views, and application pages, and enumerate lists."""

    AnonymousSearchAccessList = 14
    """Allow anonymous users to retrieve content of a list or document library through SharePoint search.
    The list permissions in the site do not change."""

    Open = 17
    """Allow access to the items contained within a site, list, or folder."""

    ViewPages = 18
    """Allow viewing of pages in a site"""

    AddAndCustomizePages = 19
    """Allow addition, modification, or deletion of HTML pages or Web Part pages, and editing of the site using a
    compatible editor."""

    ApplyThemeAndBorder = 20
    """Allow application of a theme or borders to the entire site"""

    ApplyStyleSheets = 21
    """Allow application of a style sheet (.css file) to the site """

    ViewUsageData = 22
    """Allow viewing of reports on site usage"""

    CreateSSCSite = 23
    """Allow creation of a site using Self-Service Site Creation, an implementation-specific capability."""

    ManageSubwebs = 24
    """Allow creation of a subsite within the site (2) or site collection"""

    CreateGroups = 25
    """Allow creation of a group of users that can be used anywhere within the site collection."""

    ManagePermissions = 26
    """Allow creation and modification of permission levels on the site (2) and assigning permissions to users and
    site groups."""

    BrowseDirectories = 27
    """Allow enumeration of documents and folders in a site using [MS-FPSE] and WebDAV interfaces"""

    BrowseUserInfo = 28
    """Allow viewing the information about all users of the site"""

    AddDelPrivateWebParts = 29
    """Allow addition or removal of personal Web Parts on a Web Part page."""

    UpdatePersonalWebParts = 30
    """Allow updating of Web Parts to display personalized information."""

    ManageWeb = 31
    """Allow all administration tasks for the site (2) as well as manage content."""

    AnonymousSearchAccessWebLists = 32
    """Allow content of lists and document libraries in the site (2) to be retrievable for anonymous users through
    SharePoint search if the list or document library has AnonymousSearchAccessList set."""

    UseClientIntegration = 37
    """Allow use of features that launch client applications; otherwise, users can only work on documents on their
    local machines and upload changes to the front-end Web server."""

    UseRemoteAPIs = 38
    """Allow use of SOAP, WebDAV, or [MS-FPSE] to access the site"""

    ManageAlerts = 39
    """Allow management of alerts for all users of the site"""

    CreateAlerts = 40
    """Allow creation of e-mail alerts."""

    EditMyUserInfo = 41
    """Allow users to change their own user information, such as adding a picture."""

    EnumeratePermissions = 63
    """Allow enumeration of permissions on the site, list, folder, document, or list item"""

    FullMask = 65
