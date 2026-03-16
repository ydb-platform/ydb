from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.fields.lookup_value import FieldLookupValue


class FieldMultiLookupValue(ClientValueCollection):
    def __init__(self):
        super(FieldMultiLookupValue, self).__init__(FieldLookupValue)
