from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.fields.type import FieldType


class FieldCreationInformation(ClientValue):
    def __init__(
        self,
        title,
        field_type_kind,
        description=None,
        lookup_list_id=None,
        lookup_field_name=None,
        lookup_web_id=None,
        required=False,
        formula=None,
        choices=None,
    ):
        """
        Represents metadata about fields creation.

        :param str lookup_web_id: Specifies the identifier of the site (2) that contains the list that is the
            source for the field (2) value.
        :param bool required: Specifies whether the field (2) requires a value.
        :param str lookup_field_name: Specifies the name of the field in the other data source when creating
            a lookup field.
        :param str lookup_list_id: A CSOM GUID that specifies the target list for the lookup field.
        :param str title: Specifies the display name of the field.
        :param int field_type_kind: Specifies the type of the field.
        :type description: str or None
        :param str formula:
        :param list[str] or None choices:
        """
        super(FieldCreationInformation, self).__init__()
        self.Title = title
        self.FieldTypeKind = field_type_kind
        self.Description = description
        self.Choices = (
            StringCollection(choices)
            if field_type_kind == FieldType.MultiChoice
            or field_type_kind == FieldType.Choice
            else None
        )
        self.LookupListId = lookup_list_id
        self.LookupFieldName = lookup_field_name
        self.LookupWebId = lookup_web_id
        self.Required = required
        self.Formula = formula

    @property
    def entity_type_name(self):
        return "SP.FieldCreationInformation"
