from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class ConfiguredMetadataNavigationItem(ClientValue):
    """Represents a configured metadata navigation item."""

    def __init__(
        self,
        field_display_name=None,
        field_title=None,
        field_type_as_string=None,
        is_content_type_field=None,
        is_folder_hierarchy=None,
        is_hierarchy=None,
    ):
        """
        :param str field_display_name: The display name of the field that this item refers to.
        :param str field_title: The internal name of the field that this item refers to.
        :param str field_type_as_string: The type of the field that this item refers to.
        :param bool is_content_type_field: Indicates whether the type of this field is a content type.
        :param bool is_folder_hierarchy: Indicates whether this item is a folder hierarchy.
        :param bool is_hierarchy: Indicates whether this item is hierarchical.
        """
        self.FieldDisplayName = field_display_name
        self.FieldTitle = field_title
        self.FieldTypeAsString = field_type_as_string
        self.IsContentTypeField = is_content_type_field
        self.IsFolderHierarchy = is_folder_hierarchy
        self.IsHierarchy = is_hierarchy


class ConfiguredMetadataNavigationItemCollection(ClientValue):
    """A collection of configured metadata navigation items."""

    def __init__(self, items=None):
        self.Items = ClientValueCollection(ConfiguredMetadataNavigationItem, items)
