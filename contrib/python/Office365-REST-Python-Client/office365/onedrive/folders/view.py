from office365.runtime.client_value import ClientValue


class FolderView(ClientValue):
    """The FolderView resource provides or sets recommendations on the user-experience of a folder."""

    def __init__(self, sort_by=None, sort_order=None, view_type=None):
        """
        :param str sort_by: The method by which the folder should be sorted.
        :param str sort_order: If true, indicates that items should be sorted in descending order. Otherwise,
            items should be sorted ascending.
        :param str view_type: The type of view that should be used to represent the folder.
        """
        super(FolderView, self).__init__()
        self.sortBy = sort_by
        self.sortOrder = sort_order
        self.viewType = view_type
