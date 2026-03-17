import uuid
from typing import Optional

from office365.sharepoint.fields.lookup import FieldLookup
from office365.sharepoint.taxonomy.create_xml_parameters import (
    TaxonomyFieldCreateXmlParameters,
)


class TaxonomyField(FieldLookup):
    """Represents a taxonomy field."""

    def set_field_value_by_value(self, item, tax_value):
        """
        Sets the value of the corresponding field in the list item to the value of the specified TaxonomyFieldValue
        :param ListItem item: The ListItem object whose field is to be updated.
        :param TaxonomyFieldValue tax_value:  The TaxonomyFieldValue object whose value is to be used to
            update this field.
        """

        def _set_field_value_by_value():
            item.set_property(self.internal_name, tax_value)

        self.ensure_property("InternalName", _set_field_value_by_value)
        return self

    @staticmethod
    def create(
        fields,
        name,
        term_set_id,
        term_store_id=None,
        allow_multiple_values=False,
        return_type=None,
    ):
        """
        :type fields: office365.sharepoint.fields.collection.FieldCollection
        :param str name: Field name
        :param str term_set_id: Term set identifier
        :param str term_store_id: Term store identifier
        :param bool allow_multiple_values: Specifies whether the column will allow more than one value
        :param TaxonomyField return_type: Return type
        """
        if return_type is None:
            return_type = TaxonomyField(fields.context)
        fields.add_child(return_type)
        params = TaxonomyFieldCreateXmlParameters(
            name,
            term_set_id,
            term_store_id=term_store_id,
            allow_multiple_values=allow_multiple_values,
        )

        def _create_taxonomy_field_inner():
            from office365.sharepoint.lists.list import List

            if isinstance(fields.parent, List):
                parent_list = fields.parent

                def _list_loaded():
                    params.web_id = parent_list.parent_web.id
                    params.list_id = parent_list.id
                    fields.create_field_as_xml(params.schema_xml, return_type)

                fields.parent.ensure_properties(["Id", "ParentWeb"], _list_loaded)
            else:

                def _web_loaded():
                    params.web_id = fields.context.web.id
                    fields.create_field_as_xml(params.schema_xml, return_type)

                fields.context.web.ensure_property("Id", _web_loaded)

        def _after_text_field_created(text_field):
            params.text_field_id = text_field.id
            _create_taxonomy_field_inner()

        return_type._create_text_field(name).after_execute(_after_text_field_created)
        return return_type

    def _create_text_field(self, name):
        """Creates hidden text field"""
        text_field_name = "{name}".format(name=uuid.uuid4().hex)
        text_field_schema = """
                    <Field Type="Note" DisplayName="{name}_0" Hidden="TRUE" CanBeDeleted="TRUE" ShowInViewForms="FALSE"
                           CanToggleHidden="TRUE" StaticName="{text_field_name}" Name="{text_field_name}">
                    </Field>
                """.format(
            name=name, text_field_name=text_field_name
        )
        return self.parent_collection.create_field_as_xml(text_field_schema)

    @property
    def anchor_id(self):
        # type: () -> Optional[str]
        """Gets or sets the GUID of the anchor Term object for a TaxonomyField object."""
        return self.properties.get("AnchorId", None)

    @property
    def create_values_in_edit_form(self):
        # type: () -> Optional[bool]
        """
        Specifies a Boolean value that specifies whether the new Term  objects can be added to the
        TermSet while typing in the TaxonomyField editor control.
        """
        return self.properties.get("CreateValuesInEditForm", None)

    @property
    def is_anchor_valid(self):
        # type: () -> Optional[bool]
        """Gets a Boolean value that specifies whether the Term object identified by the AnchorId property is valid."""
        return self.properties.get("IsAnchorValid", None)

    @property
    def is_doc_tags_enabled(self):
        # type: () -> Optional[bool]
        """ """
        return self.properties.get("IsDocTagsEnabled", None)

    @property
    def is_keyword(self):
        # type: () -> Optional[bool]
        """
        Specifies a Boolean value that indicates whether the TaxonomyField value points to the
        keywords term set  object.
        """
        return self.properties.get("IsKeyword", None)

    @property
    def is_path_rendered(self):
        # type: () -> Optional[bool]
        """
        Specifies a Boolean value that specifies whether the default Label objects of all the parent
        Term objects of a Term in the TaxonomyField object will be rendered in
        addition to the default label of that Term.
        """
        return self.properties.get("IsPathRendered", None)

    @property
    def is_term_set_valid(self):
        # type: () -> Optional[bool]
        """
        Gets a Boolean value that specifies whether the TermSet object identified by the TermSetId
        property exists and is available for tagging.
        """
        return self.properties.get("IsTermSetValid", None)

    @property
    def open(self):
        # type: () -> Optional[bool]
        """
        Specifies a Boolean value that specifies whether the TaxonomyField object is linked to an
        open TermSet (section 3.1.5.20) object or a closed TermSet.
        """
        return self.properties.get("Open", None)

    @property
    def ssp_id(self):
        # type: () -> Optional[str]
        """
        Specifies the GUID that identifies the TermStore object, which contains the Enterprise
        keywords for the site that the current TaxonomyField belongs to.
        """
        return self.properties.get("SspId", None)

    @property
    def target_template(self):
        # type: () -> Optional[str]
        """
        Specifies the Web-relative Uniform Resource Locator (URL) of the target page that is used to construct the
        hyperlink on each Term object when the TaxonomyField (section 3.1.5.27) object is rendered.
        """
        return self.properties.get("TargetTemplate", None)

    @property
    def term_set_id(self):
        # type: () -> Optional[str]
        """
        Specifies the GUID of the TermSet object that contains the Term
        objects used by the current TaxonomyField () object.
        """
        return self.properties.get("TermSetId", None)

    @property
    def text_field_id(self):
        # type: () -> Optional[str]
        """Gets the GUID that identifies the hidden text field in an item."""
        return self.properties.get("TextField", None)

    @property
    def text_field(self):
        """Gets the hidden text field in an item.
        :rtype: office365.sharepoint.fields.multi_line_text.FieldMultiLineText
        """
        return self.parent_collection.parent.fields.get_by_id(self.text_field_id)

    @property
    def user_created(self):
        # type: () -> Optional[bool]
        """
        Specifies a Boolean value that specifies whether the TaxonomyField object is linked to a
        customized TermSet object.
        """
        return self.properties.get("UserCreated", None)

    @property
    def entity_type_name(self):
        return "SP.Taxonomy.TaxonomyField"
