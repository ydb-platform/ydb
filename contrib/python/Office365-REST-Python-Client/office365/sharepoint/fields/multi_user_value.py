from office365.sharepoint.fields.multi_lookup_value import FieldMultiLookupValue
from office365.sharepoint.fields.user_value import FieldUserValue


class FieldMultiUserValue(FieldMultiLookupValue):
    def __init__(self):
        """Represents the multi valued user field for a list item."""
        super(FieldMultiUserValue, self).__init__()
        self._item_type = FieldUserValue
