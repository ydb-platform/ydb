from office365.runtime.client_value import ClientValue


class RenderListDataOptions:
    """The type of data to return when rendering a list view as JSON."""

    """Default render type."""
    None_ = 0

    """ Returns the list data context information. """
    ContextInfo = 1

    """ Returns the list data. """
    ListData = 2

    """ Returns the list schema. """
    ListSchema = 4

    """ Returns the menu view of the list. """
    MenuView = 8


class RenderListDataParameters(ClientValue):
    """Specifies the parameters to be used to render list data as a JSON string"""

    def __init__(
        self,
        add_all_fields=None,
        add_all_view_fields=None,
        add_regional_settings=None,
        add_required_fields=None,
        allow_multiple_value_filter_for_taxonomy_fields=None,
        audience_target=None,
        dates_in_utc=None,
        expand_groups=None,
        expand_user_field=None,
        filter_out_channel_folders_in_default_doc_lib=None,
        render_options=None,
        require_folder_coloring_fields=None,
        show_stub_file=None,
        view_xml=None,
    ):
        """
        :param bool add_all_fields:
        :param bool add_all_view_fields:
        :param bool add_regional_settings:
        :param bool add_required_fields: This parameter indicates if we return required fields.
        :param bool allow_multiple_value_filter_for_taxonomy_fields: This parameter indicates whether multi value
            filtering is allowed for taxonomy fields.
        :param bool audience_target:
        :param bool dates_in_utc: Specifies if the DateTime field is returned in UTC or local time.
        :param bool expand_groups: Specifies whether to expand the grouping or not.
        :param bool expand_user_field:
        :param bool filter_out_channel_folders_in_default_doc_lib:
        :param int render_options: Specifies the type of output to return.
        :param bool require_folder_coloring_fields:
        :param bool show_stub_file:
        :param str view_xml: Specifies the CAML view XML.
        """
        self.AddAllFields = add_all_fields
        self.AddAllViewFields = add_all_view_fields
        self.AddRegionalSettings = add_regional_settings
        self.AddRequiredFields = add_required_fields
        self.AllowMultipleValueFilterForTaxonomyFields = (
            allow_multiple_value_filter_for_taxonomy_fields
        )
        self.AudienceTarget = audience_target
        self.DatesInUtc = dates_in_utc
        self.ExpandGroups = expand_groups
        self.ExpandUserField = expand_user_field
        self.FilterOutChannelFoldersInDefaultDocLib = (
            filter_out_channel_folders_in_default_doc_lib
        )
        self.RenderOptions = render_options
        self.RequireFolderColoringFields = require_folder_coloring_fields
        self.ShowStubFile = show_stub_file
        self.ViewXml = view_xml

    @property
    def entity_type_name(self):
        return "SP.RenderListDataParameters"
