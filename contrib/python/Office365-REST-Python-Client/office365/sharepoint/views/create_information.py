from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class ViewCreationInformation(ClientValue):
    """Specifies the properties used to create a new list view."""

    def __init__(
        self,
        title=None,
        view_type_kind=None,
        view_fields=None,
        view_data=None,
        row_limit=None,
        query=None,
        personal_view=None,
        paged=None,
    ):
        """
        :param str title: Specifies the display name of the new list view. Its length MUST be equal to or less than 255.
        :param int view_type_kind: Specifies the type of the new list view.
        :param list[str] view_fields: Specifies the collection of field internal names for the list fields in
            the new list view
        :param str view_data:
        :param int row_limit: Specifies the maximum number of list items that the new list view displays on a visual
            page of the list view. Its value MUST be equal to or less than 2147483647.
        :param str query: Specifies the query for the new list view.
        :param bool personal_view: Specifies whether the new list view is a personal view.
            If the value is "false", the new list view is a public view.
        :param bool paged: Specifies whether the new list view is a paged view.
        """
        super(ViewCreationInformation, self).__init__()
        self.Title = title
        self.ViewTypeKind = view_type_kind
        self.ViewFields = StringCollection(view_fields)
        self.ViewData = view_data
        self.RowLimit = row_limit
        self.Query = query
        self.PersonalView = personal_view
        self.Paged = paged

    @property
    def entity_type_name(self):
        return "SP.ViewCreationInformation"
