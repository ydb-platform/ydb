import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class ProductTypeDefinitions(Client):
    """
    ProductTypeDefinitions SP-API Client
    :link: 

    The Selling Partner API for Product Type Definitions provides programmatic access to attribute and data requirements for product types in the Amazon catalog. Use this API to return the JSON Schema for a product type that you can then use with other Selling Partner APIs, such as the Selling Partner API for Listings Items, the Selling Partner API for Catalog Items, and the Selling Partner API for Feeds (for JSON-based listing feeds).
    """


    @sp_endpoint('/definitions/2020-09-01/productTypes', method='GET')
    def search_definitions_product_types(self, **kwargs) -> ApiResponse:
        """
        search_definitions_product_types(self, **kwargs) -> ApiResponse

        Search for and return a list of Amazon product types that have definitions available.

        **Usage Plans:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key keywords:array |  A comma-delimited list of keywords to search product types by.
            key marketplaceIds:array | * REQUIRED A comma-delimited list of Amazon marketplace identifiers for the request.

        Returns:
            ApiResponse:
        """
    
        return self._request(kwargs.pop('path'),  params=kwargs)
    

    @sp_endpoint('/definitions/2020-09-01/productTypes/{}', method='GET')
    def get_definitions_product_type(self, productType, **kwargs) -> ApiResponse:
        """
        get_definitions_product_type(self, productType, **kwargs) -> ApiResponse

        Retrieve an Amazon product type definition.

        **Usage Plans:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       10
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            productType:string | * REQUIRED The Amazon product type name.
            key sellerId:string |  A selling partner identifier. When provided, seller-specific requirements and values are populated within the product type definition schema, such as brand names associated with the selling partner.
            key marketplaceIds:array | * REQUIRED A comma-delimited list of Amazon marketplace identifiers for the request.
            key productTypeVersion:string |  The version of the Amazon product type to retrieve. Defaults to "LATEST",. Prerelease versions of product type definitions may be retrieved with "RELEASE_CANDIDATE". If no prerelease version is currently available, the "LATEST" live version will be provided.
            key requirements:string |  The name of the requirements set to retrieve requirements for.
            key requirementsEnforced:string |  Identifies if the required attributes for a requirements set are enforced by the product type definition schema. Non-enforced requirements enable structural validation of individual attributes without all the required attributes being present (such as for partial updates).
            key locale:string |  Locale for retrieving display labels and other presentation details. Defaults to the default language of the first marketplace in the request.

        Returns:
            ApiResponse:
        """
    
        return self._request(fill_query_params(kwargs.pop('path'), productType), params=kwargs)
    
