from office365.runtime.client_value import ClientValue


class ChangeQuery(ClientValue):
    """Defines a query that is performed against the change log."""

    def __init__(
        self,
        alert=False,
        site=False,
        web=False,
        list_=False,
        item=False,
        activity=False,
        file=False,
        folder=False,
        user=False,
        group=False,
        view=False,
        content_type=False,
        add=True,
        update=True,
        system_update=True,
        delete_object=True,
        role_assignment_add=True,
        role_assignment_delete=True,
        change_token_start=None,
        change_token_end=None,
        fetch_limit=None,
    ):
        """
        :param int fetch_limit:
        :param role_assignment_delete: Specifies whether deleting role assignments is included in the query.
        :param role_assignment_add: Specifies whether adding role assignments is included in the query.
        :param bool item: Gets or sets a value that specifies whether general changes to list items are included
             in the query.
        :param bool delete_object: Gets or sets a value that specifies whether delete changes are included in the query.
        :param bool content_type: Gets or sets a value that specifies whether changes to content types are included
            in the query.
        :param bool alert: Gets or sets a value that specifies whether changes to alerts are included in the query.
        :param bool add: Gets or sets a value that specifies whether add changes are included in the query.
        :param bool view: Gets or sets a value that specifies whether changes to views are included in the query.
        :param bool system_update: Gets or sets a value that specifies whether updates made using the item SystemUpdate
            method are included in the query.
        :param bool update: Gets or sets a value that specifies whether update changes are included in the query.
        :param bool user: Gets or sets a value that specifies whether changes to users are included in the query.
        :param bool folder: Gets or sets value that specifies whether changes to folders are included in the query.
        :param bool file: Gets or sets a value that specifies whether changes to files are included in the query.
        :param change_token_start: office365.sharepoint.changes.changeToken.ChangeToken
        :param change_token_end: office365.sharepoint.changes.changeToken.ChangeToken
        :param bool activity:
        :param bool site: Gets or sets a value that specifies whether changes to site collections
            are included in the query.
        :param bool web: Gets or sets a value that specifies whether changes to Web sites are included in the query.
        :param bool list_: Gets or sets a value that specifies whether changes to lists are included in the query.
        """
        super(ChangeQuery, self).__init__()
        self.Item = item
        self.Alert = alert
        self.ContentType = content_type
        self.Web = web
        self.Site = site
        self.List = list_
        self.Activity = activity
        self.File = file
        self.Folder = folder
        self.User = user
        self.Group = group
        self.View = view
        self.Add = add
        self.Update = update
        self.SystemUpdate = system_update
        self.ChangeTokenStart = change_token_start
        self.ChangeTokenEnd = change_token_end
        self.DeleteObject = delete_object
        self.RoleAssignmentAdd = role_assignment_add
        self.RoleAssignmentDelete = role_assignment_delete
        self.FetchLimit = str(fetch_limit) if fetch_limit else None

    @property
    def entity_type_name(self):
        return "SP.ChangeQuery"
