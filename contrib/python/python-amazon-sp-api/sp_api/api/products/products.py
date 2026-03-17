from typing import Optional, List, Dict, Union

from sp_api.base import ApiResponse, Client, fill_query_params, sp_endpoint
from sp_api.api.products.products_definitions import GetItemOffersBatchRequest, GetListingOffersBatchRequest


class Products(Client):
    """
    :link: https://github.com/amzn/selling-partner-api-docs/blob/main/references/product-pricing-api/productPricingV0.md
    """

    @sp_endpoint('/products/pricing/v0/price', method='GET')
    def get_product_pricing_for_skus(self, seller_sku_list: [str], item_condition=None, offer_type=None, **kwargs) -> ApiResponse:
        """
        get_product_pricing_for_skus(self, seller_sku_list: [str], item_condition: str = None, **kwargs) -> ApiResponse
        Returns pricing information for a seller's offer listings based on SKU.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                      1
        ======================================  ==============

        Examples:
            literal blocks::

                Products().get_product_pricing_for_skus(['sku', 'sku1'], MarketplaceId="ATVPDKIKX0DER")

        Args:
            seller_sku_list: [str]
            item_condition: str ("New", "Used", "Collectible", "Refurbished", "Club")
            offer_type: str ("B2C" or "B2B") Default is B2C.
            **kwargs:

        Returns:
            ApiResponse:
        """
        if item_condition is not None:
            kwargs['ItemCondition'] = item_condition
        if offer_type is not None:
            kwargs['OfferType'] = offer_type

        return self._create_get_pricing_request(seller_sku_list, 'Sku', **kwargs)

    @sp_endpoint('/products/pricing/v0/price', method='GET')
    def get_product_pricing_for_asins(self, asin_list: [str], item_condition=None, offer_type=None, **kwargs) -> ApiResponse:
        """
        get_product_pricing_for_asins(self, asin_list: [str], item_condition=None, **kwargs) -> ApiResponse
        Returns pricing information for a seller's offer listings based on ASIN.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                      1
        ======================================  ==============

        Examples:
            literal blocks::

                Products().get_product_pricing_for_asins(['asin1', 'asin2'], MarketplaceId="ATVPDKIKX0DER")

        Args:
            asin_list: [str]
            item_condition: str | ("New", "Used", "Collectible", "Refurbished", "Club") Filters the offer listings based on item condition. Possible values: New, Used, Collectible, Refurbished, Club. Available values : New, Used, Collectible, Refurbished, Club
            offer_type: str ("B2C" or "B2B") Default is B2C.
        Returns:
            ApiResponse
        """
        if item_condition is not None:
            kwargs['ItemCondition'] = item_condition
        if offer_type is not None:
            kwargs['OfferType'] = offer_type

        return self._create_get_pricing_request(asin_list, 'Asin', **kwargs)

    @sp_endpoint('/products/pricing/v0/competitivePrice', method='GET')
    def get_competitive_pricing_for_skus(self, seller_sku_list: [str], customer_type=None, **kwargs) -> ApiResponse:
        """
        get_competitive_pricing_for_skus(self, seller_sku_list, **kwargs) -> ApiResponse
        Returns competitive pricing information for a seller's offer listings based on Seller Sku

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                      1
        ======================================  ==============

        Examples:
            literal blocks::

                Products().get_competitive_pricing_for_skus([], MarketplaceId="ATVPDKIKX0DER")

        Args:
            seller_sku_list: [str]
            customer_type: Optional (query) str ("Consumer" or "Business") Indicates whether to request pricing information from the point of view of Consumer or Business buyers. Default is Consumer.

        Returns:
            ApiResponse
        """

        if customer_type is not None:
            kwargs['CustomerType'] = customer_type

        return self._create_get_pricing_request(seller_sku_list, 'Sku', **kwargs)

    @sp_endpoint('/products/pricing/v0/competitivePrice', method='GET')
    def get_competitive_pricing_for_asins(self, asin_list: [str], customer_type=None, **kwargs) -> ApiResponse:
        """
        get_competitive_pricing_for_asins(self, asin_list, **kwargs) -> ApiResponse
        Returns competitive pricing information for a seller's offer listings based on ASIN

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                      1
        ======================================  ==============

        Examples:
            literal blocks::

                Products().get_competitive_pricing_for_asins([], MarketplaceId="ATVPDKIKX0DER")

        Args:
            asin_list: [str]
            customer_type: Optional (query) str ("Consumer" or "Business") Indicates whether to request pricing information from the point of view of Consumer or Business buyers. Default is Consumer.

        Returns:
            ApiResponse

        """
        if customer_type is not None:
            kwargs['CustomerType'] = customer_type

        return self._create_get_pricing_request(asin_list, 'Asin', **kwargs)

    @sp_endpoint('/products/pricing/v0/listings/{}/offers', method='GET')
    def get_listings_offer(self, seller_sku: str, item_condition: str, customer_type: str = None, **kwargs) -> ApiResponse:
        """
        get_listings_offer(self, seller_sku: str, **kwargs) -> ApiResponse
        Returns the lowest priced offers for a single SKU listing

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       2
        ======================================  ==============

        Args:
            key MarketplaceId: Required (query) str
            item_condition: Required (query) str | ("New", "Used", "Collectible", "Refurbished", "Club") Filters the offer listings based on item condition. Possible values: New, Used, Collectible, Refurbished, Club. Available values : New, Used, Collectible, Refurbished, Club
            seller_sku: Required (path) str
            customer_type: Optional (query) str ("Consumer" or "Business") Indicates whether to request pricing information from the point of view of Consumer or Business buyers. Default is Consumer.


        Returns:
            ApiResponse

        """
        kwargs['ItemCondition'] = item_condition

        if customer_type is not None:
            kwargs['CustomerType'] = customer_type

        return self._request(fill_query_params(kwargs.pop('path'), seller_sku), params={**kwargs})       

    @sp_endpoint('/products/pricing/v0/items/{}/offers', method='GET')
    def get_item_offers(self, asin: str, item_condition: str, customer_type: str = None, **kwargs) -> ApiResponse:
        """
        get_item_offers(self, asin: str, **kwargs) -> ApiResponse
        Returns the lowest priced offers for a single item based on ASIN

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                      1
        ======================================  ==============

        Args:
            key MarketplaceId: Required (query) str
            item_condition: Required (query) str | ("New", "Used", "Collectible", "Refurbished", "Club") Filters the offer listings based on item condition. Possible values: New, Used, Collectible, Refurbished, Club. Available values : New, Used, Collectible, Refurbished, Club
            asin: Required (path) str
            customer_type: Optional (query) str ("Consumer" or "Business") Indicates whether to request pricing information from the point of view of Consumer or Business buyers. Default is Consumer.


        Returns:
            ApiResponse

        """
        kwargs['ItemCondition'] = item_condition

        if customer_type is not None:
            kwargs['CustomerType'] = customer_type

        return self._request(fill_query_params(kwargs.pop('path'), asin), params={**kwargs})

    @sp_endpoint('/batches/products/pricing/v0/itemOffers', method='POST')
    def get_item_offers_batch(self, requests_: Optional[Union[List[Dict], GetItemOffersBatchRequest]] = None,
                              **kwargs) -> ApiResponse:
        """
        get_item_offers_batch(self, requests_: Optional[List[Union[Dict, ItemOffersRequest]]], **kwargs) -> ApiResponse
        Returns the lowest priced offers for a batch of items based on ASIN.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                       1
        ======================================  ==============

        Args:
            requests_: Optional (Body) [dict] The request associated with the getItemOffersBatch API call.


        Returns:
            ApiResponse

        """
        if isinstance(requests_, GetItemOffersBatchRequest):
            get_item_offers_batch_request = requests_.to_dict()
        else:
            get_item_offers_batch_request = {"requests": requests_}

        return self._request(kwargs.pop('path'), data=get_item_offers_batch_request, params={**kwargs},
                             add_marketplace=False)

    @sp_endpoint('/batches/products/pricing/v0/listingOffers', method='POST')
    def get_listing_offers_batch(self, requests_: Optional[Union[List[Dict], GetListingOffersBatchRequest]] = None, **kwargs) -> ApiResponse:
        """
        get_listing_offers_batch(self, requests_: Optional[Union[List[Dict], GetListingOffersBatchRequest]], **kwargs) -> ApiResponse
        Returns the lowest priced offers for a batch of listings based on ASIN.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .5                                       1
        ======================================  ==============

        Args:
            requests_: Optional (Body) [dict] The request associated with the getListingOffersBatch API call.


        Returns:
            ApiResponse

        """
        if isinstance(requests_, GetListingOffersBatchRequest):
            get_listing_offers_batch_request = requests_.to_dict()
        else:
            get_listing_offers_batch_request = {"requests": requests_}

        return self._request(kwargs.pop('path'), data=get_listing_offers_batch_request, params={**kwargs},
                             add_marketplace=False)

    def _create_get_pricing_request(self, item_list, item_type, **kwargs):
        return self._request(kwargs.pop('path'),
                             params={**{f"{item_type}s": ','.join(item_list)},
                                     'ItemType': item_type,
                                     **({'ItemCondition': kwargs.pop(
                                         'ItemCondition')} if 'ItemCondition' in kwargs else {}),
                                     **({'CustomerType': kwargs.pop(
                                         'CustomerType')} if 'CustomerType' in kwargs else {}),
                                     **({'OfferType': kwargs.pop(
                                         'OfferType')} if 'OfferType' in kwargs else {}),
                                     'MarketplaceId': kwargs.get('MarketplaceId', self.marketplace_id)})

