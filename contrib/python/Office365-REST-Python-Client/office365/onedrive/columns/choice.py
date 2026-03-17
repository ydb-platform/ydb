from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class ChoiceColumn(ClientValue):
    """
    The choiceColumn on a columnDefinition resource indicates that the column's values can be selected
    from a list of choices.
    """

    def __init__(self, allow_text_entry=True, choices=None, display_as=None):
        """
        :param bool allow_text_entry: If true, allows custom values that aren't in the configured choices.
        :param list[str] choices: The list of values available for this column.
        :param str display_as: How the choices are to be presented in the UX. Must be one of checkBoxes,
            dropDownMenu, or radioButtons
        """
        self.allowTextEntry = allow_text_entry
        self.choices = StringCollection(choices)
        self.displayAs = display_as
