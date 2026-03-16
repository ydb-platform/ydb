from datetime import datetime

from office365.runtime.client_value import ClientValue
from office365.sharepoint.fields.lookup_value import FieldLookupValue


class ListItemFormUpdateValue(ClientValue):
    """Specifies the properties of a list item field and its value."""

    def __init__(
        self,
        name=None,
        value=None,
        has_exception=None,
        error_code=None,
        error_message=None,
    ):
        """
        :param str name: Specifies the field internal name for a field.
        :param str value: Specifies a value for a field.
        :param bool has_exception: Specifies whether there was an error result after validating the value for the field
        param int ErrorCode: Specifies the error code after validating the value for the field
        """
        super(ListItemFormUpdateValue, self).__init__()
        self.FieldName = name
        self.FieldValue = value
        self.HasException = has_exception
        self.ErrorCode = error_code
        self.ErrorMessage = error_message

    def __repr__(self):
        if self.HasException:
            return "{0} update failed: Message: {1}".format(
                self.FieldName, self.ErrorMessage
            )
        else:
            return "{0} update succeeded".format(self.FieldName)

    def to_json(self, json_format=None):
        json = super(ListItemFormUpdateValue, self).to_json(json_format)
        if isinstance(self.FieldValue, FieldLookupValue):
            json["FieldValue"] = (
                "[{" + "'Key':'{0}'".format(self.FieldValue.LookupValue) + "}]"
            )
        elif isinstance(self.FieldValue, datetime):
            json["FieldValue"] = self.FieldValue.isoformat()
        return json

    @property
    def entity_type_name(self):
        return "SP.ListItemFormUpdateValue"
