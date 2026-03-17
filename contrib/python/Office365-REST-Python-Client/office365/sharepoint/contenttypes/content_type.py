from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.contenttypes.content_type_id import ContentTypeId
from office365.sharepoint.contenttypes.fieldlinks.collection import FieldLinkCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.fields.collection import FieldCollection
from office365.sharepoint.translation.user_resource import UserResource


class ContentType(Entity):
    """
    Specifies a content type.

    A named and uniquely identifiable collection of settings and fields that store metadata for individual items
    in a SharePoint list. One or more content types can be associated with a list, which restricts the contents
    to items of those types
    """

    def __str__(self):
        return self.name or self.entity_type_name

    def __repr__(self):
        return self.string_id or str(self.id) or self.entity_type_name

    def reorder_fields(self, field_names):
        """
        The ReorderFields method is called to change the order in which fields appear in a content type.
        :param list[str] field_names: Rearranges the collection of fields in the order in which field internal
             names are specified.
        """
        payload = {"fieldNames": StringCollection(field_names)}
        qry = ServiceOperationQuery(self, "ReorderFields", None, payload)
        self.context.add_query(qry)
        return self

    def update(self, update_children):
        """
        Updates the content type, and any child objects  of the content type if specified,
        with any changes made to the content type.
        :param bool update_children: Specifies whether changes propagate to child objects of the content type.
        """
        super(ContentType, self).update()
        if update_children:
            payload = {"updateChildren": update_children}
            qry = ServiceOperationQuery(self, "Update", None, payload)
            self.context.add_query(qry)
        return self

    @property
    def client_form_custom_formatter(self):
        # type: () -> Optional[str]
        return self.properties.get("ClientFormCustomFormatter", None)

    @property
    def display_form_client_side_component_id(self):
        # type: () -> Optional[str]
        """
        The component ID of an SPFx Form Customizer to connect to this content type for usage with display forms.
        """
        return self.properties.get("DisplayFormClientSideComponentId", None)

    @property
    def display_form_client_side_component_properties(self):
        # type: () -> Optional[str]
        """
        The component properties of an SPFx Form Customizer to connect to this content type for usage with display forms
        """
        return self.properties.get("DisplayFormClientSideComponentProperties", None)

    @property
    def display_form_template_name(self):
        # type: () -> Optional[str]
        """
        Specifies the name of a custom display form template to use for list items that have been assigned
        the content type.
        """
        return self.properties.get("DisplayFormTemplateName", None)

    @property
    def display_form_url(self):
        # type: () -> Optional[str]
        """
        Specifies the URL of a custom display form to use for list items that have been assigned the content type.
        """
        return self.properties.get("DisplayFormUrl", None)

    @property
    def edit_form_client_side_component_id(self):
        # type: () -> Optional[str]
        """
        The component properties of an SPFx Form Customizer to connect to this content type for usage with edit item
            forms
        """
        return self.properties.get("EditFormClientSideComponentId", None)

    @property
    def edit_form_client_side_component_properties(self):
        # type: () -> Optional[str]
        """
        The component ID of an SPFx Form Customizer to connect to this content type for usage with edit item forms
        """
        return self.properties.get("EditFormClientSideComponentProperties", None)

    @property
    def id(self):
        """
        Specifies an identifier for the content type as specified in [MS-WSSTS] section 2.1.2.8.1.
        """
        return self.properties.get("Id", ContentTypeId())

    @property
    def scope(self):
        # type: () -> Optional[str]
        """Specifies a server-relative path to the content type scope of the content type."""
        return self.properties.get("Scope", None)

    @property
    def sealed(self):
        # type: () -> Optional[bool]
        """Specifies whether the content type can be changed."""
        return self.properties.get("Sealed", None)

    @property
    def string_id(self):
        # type: () -> Optional[str]
        """A string representation of the value of the Id"""
        return self.properties.get("StringId", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Gets the name of the content type."""
        return self.properties.get("Name", None)

    @name.setter
    def name(self, value):
        # type: (str) -> None
        """Sets the name of the content type."""
        self.set_property("Name", value)

    @property
    def new_form_client_side_component_properties(self):
        # type: () -> Optional[str]
        """The component properties of an SPFx Form Customizer to connect to this content type for usage with new
        item forms"""
        return self.properties.get("NewFormClientSideComponentProperties", None)

    @property
    def new_form_url(self):
        # type: () -> Optional[str]
        """Specifies the URL of a custom new form to use for list items that have been assigned the content type."""
        return self.properties.get("NewFormUrl", None)

    @property
    def description(self):
        # type: () -> Optional[str]
        """Gets the description of the content type."""
        return self.properties.get("Description", None)

    @description.setter
    def description(self, value):
        # type: (str) -> None
        """Sets the description of the content type."""
        self.set_property("Description", value)

    @property
    def description_resource(self):
        """Gets the SP.UserResource object (section 3.2.5.333) for the description of this content type"""
        return self.properties.get(
            "DescriptionResource",
            UserResource(
                self.context, ResourcePath("DescriptionResource", self.resource_path)
            ),
        )

    @property
    def document_template(self):
        # type: () -> Optional[str]
        """Specifies the file path to the document template (1) used for a new list item that has been assigned
        the content type.
        """
        return self.properties.get("DocumentTemplate", None)

    @property
    def document_template_url(self):
        # type: () -> Optional[str]
        """Specifies the URL of the document template assigned to the content type."""
        return self.properties.get("DocumentTemplateUrl", None)

    @property
    def edit_form_url(self):
        # type: () -> Optional[str]
        """
        Specifies the URL of a custom edit form to use for list items that have been assigned the content type.
        """
        return self.properties.get("EditFormUrl", None)

    @property
    def group(self):
        # type: () -> Optional[str]
        """Gets the group of the content type."""
        return self.properties.get("Group", None)

    @group.setter
    def group(self, value):
        # type: (str) -> None
        """Sets the group of the content type."""
        self.set_property("Group", value)

    @property
    def hidden(self):
        # type: () -> Optional[bool]
        """Specifies whether the content type is unavailable for creation or usage directly from a user interface."""
        return self.properties.get("Hidden", None)

    @property
    def js_link(self):
        # type: () -> Optional[str]
        """Gets or sets the JSLink for the content type custom form template"""
        return self.properties.get("JSLink", None)

    @property
    def read_only(self):
        # type: () -> Optional[bool]
        """Specifies whether changes to the content type properties are denied."""
        return self.properties.get("ReadOnly", None)

    @property
    def name_resource(self):
        """Specifies the SP.UserResource object for the name of this content type"""
        return self.properties.get(
            "NameResource",
            UserResource(
                self.context, ResourcePath("NameResource", self.resource_path)
            ),
        )

    @property
    def schema_xml(self):
        # type: () -> Optional[str]
        """Specifies the XML schema that represents the content type."""
        return self.properties.get("SchemaXml", None)

    @property
    def fields(self):
        """Gets a value that specifies the collection of fields for the content type."""
        return self.properties.get(
            "Fields",
            FieldCollection(self.context, ResourcePath("Fields", self.resource_path)),
        )

    @property
    def parent(self):
        """Gets the parent content type of the content type."""
        return self.properties.get(
            "Parent",
            ContentType(self.context, ResourcePath("Parent", self.resource_path)),
        )

    @property
    def field_links(self):
        """Specifies the collection of field links for the content type."""
        return self.properties.get(
            "FieldLinks",
            FieldLinkCollection(
                self.context, ResourcePath("FieldLinks", self.resource_path)
            ),
        )

    @property
    def property_ref_name(self):
        return "StringId"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "DescriptionResource": self.description_resource,
                "FieldLinks": self.field_links,
                "NameResource": self.name_resource,
            }
            default_value = property_mapping.get(name, None)
        return super(ContentType, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(ContentType, self).set_property(name, value, persist_changes)
        # fallback: create a new resource path
        if name == self.property_ref_name and self._resource_path is None:
            self._resource_path = self.parent_collection.get_by_id(value).resource_path
        return self
