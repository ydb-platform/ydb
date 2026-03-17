from office365.runtime.client_value import ClientValue


class ChangeLogItemQuery(ClientValue):
    def __init__(
        self,
        change_token=None,
        query=None,
        query_options=None,
        contains=None,
        row_limit=None,
    ):
        """
        Specifies an object that is used as the input parameter of
        GetListItemChangesSinceToken (section 3.2.5.79.2.1.7) method.

        :param str change_token: Specifies a string that contains the change token for the request.
        :param str query: Specifies which records from the list are to be returned and the order in which they will be
            returned. See section 2.2 in [MS-WSSCAML].
        :param str query_options: Specifies various options for modifying the Query (section 3.2.5.183.1.1.3).
             See [MS-LISTSWS] section 2.2.4.4.
        :param str contains: Specifies a string representation of the XML element that defines custom filtering
            for the query. See [MS-LISTSWS] section 2.2.4.3.
        :param int row_limit: Specifies a limit for the number of items in the query that are returned per page.
        """
        super(ChangeLogItemQuery, self).__init__()
        self.Query = query
        self.QueryOptions = query_options
        self.ChangeToken = change_token
        self.Contains = contains
        self.RowLimit = str(row_limit) if row_limit else None

    @property
    def entity_type_name(self):
        return "SP.ChangeLogItemQuery"
