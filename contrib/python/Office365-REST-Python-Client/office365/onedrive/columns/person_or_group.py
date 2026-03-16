from office365.runtime.client_value import ClientValue


class PersonOrGroupColumn(ClientValue):
    """The personOrGroupColumn on a columnDefinition resource indicates that the column's values represent
    a person or group chosen from the directory."""

    def __init__(
        self, allow_multiple_selection=None, choose_from_type=None, display_as=None
    ):
        """
        :param bool allow_multiple_selection: Indicates whether multiple values can be selected from the source.
        :param str choose_from_type: Whether to allow selection of people only, or people and groups.
             Must be one of peopleAndGroups or peopleOnly.
        :param str display_as: How to display the information about the person or group chosen.
        """
        self.allowMultipleSelection = allow_multiple_selection
        self.chooseFromType = choose_from_type
        self.displayAs = display_as
