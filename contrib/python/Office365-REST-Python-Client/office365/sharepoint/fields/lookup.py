from typing import Optional

from office365.sharepoint.fields.field import Field


class FieldLookup(Field):
    """Specifies a lookup field."""

    @property
    def allow_multiple_values(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the lookup field allows multiple values."""
        return self.properties.get("AllowMultipleValues", None)

    @allow_multiple_values.setter
    def allow_multiple_values(self, value):
        # type: (bool) -> None
        """Sets a value that specifies whether the lookup field allows multiple values."""
        self.set_property("AllowMultipleValues", value)

    @property
    def lookup_web_id(self):
        # type: () -> Optional[str]
        """Gets the ID of the Web site that contains the list that is the source of this field's value."""
        return self.properties.get("LookupWebId", None)

    @lookup_web_id.setter
    def lookup_web_id(self, val):
        # type: (str) -> None
        """Sets the ID of the Web site that contains the list that is the source of this field's value."""
        self.set_property("LookupWebId", val, True)

    @property
    def lookup_list(self):
        # type: () -> Optional[str]
        """Gets value that specifies the list identifier of the list that contains the field to use as the lookup
        values."""
        return self.properties.get("LookupList", None)

    @lookup_list.setter
    def lookup_list(self, val):
        # type: (str) -> None
        """Sets a value that specifies the list identifier of the list that contains the field to use as
        the lookup values."""
        self.set_property("LookupList", val, True)

    @property
    def primary_field_id(self):
        # type: () -> Optional[str]
        """Specifies the primary lookup field identifier if this is a dependent lookup field.
        Otherwise, it is an empty string."""
        return self.properties.get("PrimaryFieldId", None)

    @property
    def relationship_delete_behavior(self):
        # type: () -> Optional[int]
        """Gets a value that specifies the relationship delete behavior of the lookup field."""
        return self.properties.get("RelationshipDeleteBehavior", None)

    @property
    def unlimited_length_in_document_library(self):
        # type: () -> Optional[bool]
        """Gets or sets a Boolean value that specifies whether to allow values with unlimited text
        in the lookup field."""
        return self.properties.get("UnlimitedLengthInDocumentLibrary", None)
