from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.fields.type import FieldType
from office365.sharepoint.translation.user_resource import UserResource


class Field(Entity):
    """A container for metadata within a SharePoint list and associated list items."""

    def __init__(self, context, resource_path=None):
        super(Field, self).__init__(context, resource_path)

    def __str__(self):
        return self.title or self.entity_type_name

    def __repr__(self):
        return self.internal_name or self.id or self.entity_type_name

    @staticmethod
    def resolve_field_type(type_id_or_name):
        from office365.sharepoint.fields.calculated import FieldCalculated
        from office365.sharepoint.fields.choice import FieldChoice
        from office365.sharepoint.fields.computed import FieldComputed
        from office365.sharepoint.fields.currency import FieldCurrency
        from office365.sharepoint.fields.date_time import FieldDateTime
        from office365.sharepoint.fields.guid import FieldGuid
        from office365.sharepoint.fields.lookup import FieldLookup
        from office365.sharepoint.fields.multi_choice import FieldMultiChoice
        from office365.sharepoint.fields.multi_line_text import FieldMultiLineText
        from office365.sharepoint.fields.text import FieldText
        from office365.sharepoint.fields.url import FieldUrl
        from office365.sharepoint.fields.user import FieldUser
        from office365.sharepoint.taxonomy.field import TaxonomyField

        field_known_types = {
            FieldType.Text: FieldText,
            FieldType.Calculated: FieldCalculated,
            FieldType.Choice: FieldChoice,
            FieldType.MultiChoice: FieldMultiChoice,
            FieldType.Lookup: FieldLookup,
            FieldType.User: FieldUser,
            FieldType.Computed: FieldComputed,
            FieldType.URL: FieldUrl,
            FieldType.Guid: FieldGuid,
            FieldType.Currency: FieldCurrency,
            FieldType.Note: FieldMultiLineText,
            FieldType.DateTime: FieldDateTime,
        }
        if isinstance(type_id_or_name, int):
            return field_known_types.get(type_id_or_name, Field)
        else:
            if (
                type_id_or_name == "TaxonomyFieldType"
                or type_id_or_name == "TaxonomyFieldTypeMulti"
            ):
                return TaxonomyField
            elif type_id_or_name == "Thumbnail":
                from office365.sharepoint.fields.thumbnail import FieldThumbnail

                return FieldThumbnail
            else:
                return Field

    @staticmethod
    def create_field(context, field_parameters):
        """
        Creates a field based on its type

        :type context: office365.sharepoint.client_context.ClientContext
        :type field_parameters: office365.sharepoint.fields.creation_information.FieldCreationInformation
        """
        field_type = Field.resolve_field_type(field_parameters.FieldTypeKind)
        field = field_type(context)
        for n, v in field_parameters.to_json().items():
            field.set_property(n, v)
        return field

    def enable_index(self):
        """
        Enables the index for a field
        An index speeds up queries on the indexed fields as well as sorting and grouping operations
        """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "enableIndex", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def disable_index(self):
        """
        Disables the index for a field
        """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "disableIndex", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def set_show_in_display_form(self, flag):
        """Sets the value of the ShowInDisplayForm property for this fields.

        :type flag: bool
        """
        qry = ServiceOperationQuery(self, "setShowInDisplayForm", [flag])
        self.context.add_query(qry)
        return self

    def set_show_in_edit_form(self, flag):
        """Sets the value of the ShowInEditForm property for this fields.

        :param bool flag: A Boolean value that indicates whether this field is shown in the edit form.
        """
        qry = ServiceOperationQuery(self, "setShowInEditForm", [flag])
        self.context.add_query(qry)

    def set_show_in_new_form(self, flag):
        """Sets the value of the ShowInNewForm property for this fields.

        :param bool flag: A Boolean value that indicates whether this field is shown in the new form.
        """
        qry = ServiceOperationQuery(self, "setShowInNewForm", [flag])
        self.context.add_query(qry)
        return self

    @property
    def id(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the field identifier."""
        return self.properties.get("Id", None)

    @property
    def auto_indexed(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether the field is auto-indexed."""
        return self.properties.get("AutoIndexed", None)

    @property
    def default_formula(self):
        # type: () -> Optional[str]
        return self.properties.get("DefaultFormula", None)

    @property
    def description_resource(self):
        """Gets the resource object corresponding to the Description property for a field"""
        return self.properties.get(
            "DescriptionResource",
            UserResource(
                self.context, ResourcePath("DescriptionResource", self.resource_path)
            ),
        )

    @property
    def schema_xml(self):
        # type: () -> Optional[str]
        """
        Gets a value that specifies the XML schema that defines the field.
        """
        return self.properties.get("SchemaXml", None)

    @schema_xml.setter
    def schema_xml(self, val):
        """Sets a value that specifies the XML schema that defines the field."""
        self.set_property("SchemaXml", val)

    @property
    def type_as_string(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the type of the field"""
        return self.properties.get("TypeAsString", None)

    @property
    def title(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the display name of the field."""
        return self.properties.get("Title", None)

    @title.setter
    def title(self, val):
        """
        Sets a value that specifies the display name of the field.
        """
        self.set_property("Title", val)

    @property
    def group(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the field group"""
        return self.properties.get("Group", None)

    @group.setter
    def group(self, val):
        """Sets a value that specifies the field group."""
        self.set_property("Group", val)

    @property
    def internal_name(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the field internal name."""
        return self.properties.get("InternalName", None)

    @property
    def can_be_deleted(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the field can be deleted"""
        return self.properties.get("CanBeDeleted", None)

    @property
    def client_side_component_id(self):
        # type: () -> Optional[str]
        return self.properties.get("ClientSideComponentId", None)

    @property
    def client_side_component_properties(self):
        # type: () -> Optional[str]
        return self.properties.get("ClientSideComponentProperties", None)

    @property
    def client_validation_formula(self):
        # type: () -> Optional[str]
        return self.properties.get("ClientValidationFormula", None)

    @property
    def enforce_unique_values(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the field enforces unique values.
        """
        return self.properties.get("enforceUniqueValues", None)

    @property
    def filterable(self):
        # type: () -> Optional[bool]
        """
        Specifies whether list items in the list can be filtered by the field value.
        """
        return self.properties.get("Filterable", None)

    @property
    def from_base_type(self):
        # type: () -> Optional[bool]
        """
        Gets a Boolean value that indicates whether the field derives from a base field type.
        """
        return self.properties.get("FromBaseType", None)

    @property
    def js_link(self):
        # type: () -> Optional[str]
        """
        When implemented in a derived class, gets or sets the name of an external JavaScript file that contains
        any client rendering logic for fields of the derived type.
        """
        return self.properties.get("JSLink", None)

    @property
    def hidden(self):
        # type: () -> Optional[bool]
        """
        Gets a value that specifies whether the field is hidden in list views and list forms.
        """
        return self.properties.get("Hidden", None)

    @hidden.setter
    def hidden(self, val):
        """
        Sets a value that specifies whether the field is hidden in list views and list forms.
        """
        self.set_property("Hidden", val)

    @property
    def no_crawl(self):
        # type: () -> Optional[bool]
        """
        Gets value that specifies whether the field can be crawled by a search engine.
        """
        return self.properties.get("NoCrawl", None)

    @property
    def read_only_field(self):
        # type: () -> Optional[bool]
        """
        Specifies whether the value of the field is read-only.
        """
        return self.properties.get("ReadOnlyField", None)

    @property
    def default_value(self):
        # type: () -> Optional[str]
        """
        Gets  a value that specifies the default value for the field.
        """
        return self.properties.get("DefaultValue", None)

    @default_value.setter
    def default_value(self, val):
        """
        Sets a value that specifies the default value for the field.
        """
        self.set_property("DefaultValue", val)

    @property
    def indexed(self):
        return self.properties.get("Indexed", None)

    @property
    def type_display_name(self):
        # type: () -> Optional[str]
        """
        Gets a value that specifies the display name for the type of the field.
        """
        return self.properties.get("TypeDisplayName", None)

    @property
    def title_resource(self):
        """Gets the resource object corresponding to the Title property for a field"""
        return self.properties.get(
            "TitleResource",
            UserResource(
                self.context, ResourcePath("TitleResource", self.resource_path)
            ),
        )

    @property
    def type_short_description(self):
        # type: () -> Optional[str]
        """
        Gets a value that specifies the description for the type of the field.
        """
        return self.properties.get("TypeShortDescription", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "DescriptionResource": self.description_resource,
                "TitleResource": self.title_resource,
            }
            default_value = property_mapping.get(name, None)
        return super(Field, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(Field, self).set_property(name, value, persist_changes)
        # fallback: create a new resource path
        if name == "Id" and self._resource_path is None:
            self._resource_path = self.parent_collection.get_by_id(value).resource_path
        if name == "FieldTypeKind":
            self.__class__ = self.resolve_field_type(value)
        elif name == "TypeAsString" and self.properties.get("FieldTypeKind", 0) == 0:
            self.__class__ = self.resolve_field_type(value)
        return self
