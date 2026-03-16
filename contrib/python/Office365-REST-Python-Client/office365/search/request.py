from typing import Dict, Optional

from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.odata.json_format import ODataJsonFormat
from office365.runtime.types.collections import StringCollection
from office365.search.aggregation_option import AggregationOption
from office365.search.sharepoint_onedrive_options import SharePointOneDriveOptions
from office365.search.sort_property import SortProperty


class SearchRequest(ClientValue):
    """A search request formatted in a JSON blob."""

    def __init__(
        self,
        query,
        aggregation_filters=None,
        aggregations=None,
        enable_top_results=None,
        size=None,
        region=None,
        entity_types=None,
        fields=None,
        page_from=None,
        sort_properties=None,
        content_sources=None,
        sharepoint_onedrive_options=SharePointOneDriveOptions(),
    ):
        """
        :param office365.search.query.SearchQuery query: Contains the query terms.
        :param list[str] aggregation_filters: Contains one or more filters to obtain search results aggregated and
            filtered to a specific value of a field
        :param list[str] aggregations: Specifies aggregations (also known as refiners) to be returned
            alongside search results.
        :param bool enable_top_results: This triggers hybrid sort for messages : the first 3 messages are
            the most relevant. This property is only applicable to entityType=message. Optional.
        :param int size: The size of the page to be retrieved. The maximum value is 500. Optional.
        :param str region: The geographic location for the search. Required for searches that use application
            permissions. For details, see Get the region value.
        :param list[str] entity_types: One or more types of resources expected in the response.
            Possible values are: list, site, listItem, message, event, drive, driveItem, externalItem.
            See known limitations for those combinations of two or more entity types that are supported in the
            same search request.
        :param list[str] fields: Contains the fields to be returned for each resource object specified in entityTypes,
            allowing customization of the fields returned by default; otherwise, including additional fields such
            as custom managed properties from SharePoint and OneDrive, or custom fields in externalItem from the
            content that Microsoft Graph connectors bring in. The fields property can use the semantic labels
            applied to properties. For example, if a property is labeled as title, you can retrieve it using
            the following syntax: label_title.
        :param int page_from: Specifies the offset for the search results. Offset 0 returns the very first result.
        :param list[SortProperty] sort_properties: Contains the ordered collection of fields and direction to
            sort results. There can be at most 5 sort properties in the collection.
        :param list[str] content_sources: Contains the connection to be targeted.
        """
        super(SearchRequest, self).__init__()
        self.query = query
        self.aggregationFilters = StringCollection(aggregation_filters)
        self.aggregations = ClientValueCollection(AggregationOption, aggregations)
        self.enableTopResults = enable_top_results
        self.size = size
        self.region = region
        self.entityTypes = entity_types
        self.fields = fields
        self.page_from = page_from
        self.sortProperties = ClientValueCollection(SortProperty, sort_properties)
        self.contentSources = StringCollection(content_sources)
        self.sharePointOneDriveOptions = sharepoint_onedrive_options

    def to_json(self, json_format=None):
        # type: (Optional[ODataJsonFormat]) -> Dict
        json_value = super(SearchRequest, self).to_json(json_format)
        json_value["from"] = json_value.pop("page_from", None)
        return json_value
