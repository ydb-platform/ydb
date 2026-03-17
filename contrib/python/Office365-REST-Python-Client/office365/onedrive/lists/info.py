from office365.runtime.client_value import ClientValue


class ListInfo(ClientValue):
    def __init__(self, template=None, content_types_enabled=False, hidden=False):
        """
        The listInfo complex type provides additional information about a list.

        :param str template: An enumerated value that represents the base list template used in creating the list.
            Possible values include documentLibrary, genericList, task, survey, announcements, contacts, and more.
        :param bool content_types_enabled: If true, indicates that content types are enabled for this list.
        :param bool hidden: 	If true, indicates that the list is not normally visible in the SharePoint user
            experience.
        """
        super(ListInfo, self).__init__()
        self.template = template
        self.contentTypesEnabled = content_types_enabled
        self.hidden = hidden
