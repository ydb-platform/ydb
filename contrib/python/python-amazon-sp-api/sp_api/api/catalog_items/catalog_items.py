import enum

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class CatalogItemsVersion(str, enum.Enum):
    V_2020_12_01 = "2020-12-01"
    V_2022_04_01 = "2022-04-01"
    LATEST = "2022-04-01"


class CatalogItems(Client):
    """
    CatalogItems SP-API Client
    :link:

    The Selling Partner API for Catalog Items provides programmatic access to information about items in the Amazon catalog.
    """

    version: CatalogItemsVersion = CatalogItemsVersion.V_2020_12_01

    def __init__(self, *args, **kwargs):
        if 'version' in kwargs:
            self.version = kwargs.get('version', CatalogItemsVersion.V_2020_12_01)
        super().__init__(*args, **{**kwargs, 'version': self.version})

    @sp_endpoint('/catalog/<version>/items', method='GET')
    def search_catalog_items(self, **kwargs) -> ApiResponse:
        """
        search_catalog_items(self, **kwargs) -> ApiResponse

        Search for and return a list of Amazon catalog items and associated information.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key keywords:array | * REQUIRED A comma-delimited list of words or item identifiers to search the Amazon catalog for.
            key marketplaceIds:array | * REQUIRED A comma-delimited list of Amazon marketplace identifiers for the request.
            key includedData:array |  A comma-delimited string or list of data sets to include in the response. Default: summaries.
            key brandNames:array |  A comma-delimited list of brand names to limit the search to.
            key classificationIds:array |  A comma-delimited list of classification identifiers to limit the search to.
            key pageSize:integer |  Number of results to be returned per page.
            key pageToken:string |  A token to fetch a certain page when there are multiple pages worth of results.
            key keywordsLocale:string |  The language the keywords are provided in. Defaults to the primary locale of the marketplace.
            key locale:string |  Locale for retrieving localized summaries. Defaults to the primary locale of the marketplace.

        Returns:
            ApiResponse:
        """

        includedData = kwargs.get('includedData', [])
        if includedData and isinstance(includedData, list):
            kwargs['includedData'] = ','.join(includedData)
        return self._request(kwargs.pop('path'),  params=kwargs)

    @sp_endpoint('/catalog/<version>/items/{}', method='GET')
    def get_catalog_item(self, asin, **kwargs) -> ApiResponse:
        """
        get_catalog_item(self, asin, **kwargs) -> ApiResponse

        Retrieves details for an item in the Amazon catalog.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       5
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            asin:string | * REQUIRED The Amazon Standard Identification Number (ASIN) of the item.
            key marketplaceIds:array | * REQUIRED A comma-delimited list of Amazon marketplace identifiers. Data sets in the response contain data only for the specified marketplaces.
            key includedData:array |  A comma-delimited string or list of data sets to include in the response. Default: summaries.
            key locale:string |  Locale for retrieving localized summaries. Defaults to the primary locale of the marketplace.

        Returns:
            ApiResponse:
        """

        includedData = kwargs.get('includedData', [])
        if includedData and isinstance(includedData, list):
            kwargs['includedData'] = ','.join(includedData)
        return self._request(fill_query_params(kwargs.pop('path'), asin), params=kwargs)
