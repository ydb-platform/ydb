from office365.runtime.client_value import ClientValue
from office365.runtime.odata.type import ODataType
from office365.sharepoint.search.simple_data_table import SimpleDataTable


class RelevantResults(ClientValue):
    """
    The RelevantResults table contains the actual query results. It MUST only be present if the ResultTypes element
    in the properties element of the Execute message contains ResultType.RelevantResults,
    as specified in section 2.2.5.5
    """

    def __init__(
        self,
        group_template_id=None,
        item_template_id=None,
        properties=None,
        result_title=None,
        result_title_url=None,
        table=SimpleDataTable(),
        row_count=None,
        total_rows=None,
        total_rows_including_duplicates=None,
    ):
        """
        :param str item_template_id: Specifies the identifier of the layout template that specifies how the result
            item will be displayed.
        :param str group_template_id: Specifies the identifier of the layout template that specifies how the results
            returned will be arranged.
        :param dict properties: Specifies a property bag of key value pairs
        :param str result_title: Specifies the title associated with results for the transformed query by query rule
            action. MUST NOT be more than 64 characters in length.
        :param str result_title_url: Specifies the URL to be linked to the ResultTitle. MUST NOT be more than 2048
            characters in length
        :param SimpleDataTable table: This contains a table of query results
        :param int row_count: The number of query results contained in the Table element.
        :param int total_rows: This element MUST contain the total number of results that match the conditions given
             in the search query and are of the type specified in the ResultType element.
        :param int total_rows_including_duplicates:  This element SHOULD contain the total number of results,
            including duplicates, that match the conditions given in the search query and are of the type specified
            in the ResultType element
        """
        super(RelevantResults, self).__init__()
        self.GroupTemplateId = group_template_id
        self.ItemTemplateId = item_template_id
        self.Properties = properties
        self.ResultTitle = result_title
        self.ResultTitleUrl = result_title_url
        self.RowCount = row_count
        self.Table = table
        self.TotalRows = total_rows
        self.TotalRowsIncludingDuplicates = total_rows_including_duplicates

    def set_property(self, k, v, persist_changes=True):
        if k == "Properties":
            v = ODataType.parse_key_value_collection(v)
        super(RelevantResults, self).set_property(k, v, persist_changes)
        return self

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.RelevantResults"
