from typing import List
from urllib.parse import quote_plus

from sp_api.base.helpers import sp_endpoint, fill_query_params
from sp_api.base import Client, ApiResponse


class ProductFees(Client):
    """
    :link: https://github.com/amzn/selling-partner-api-docs/tree/main/references/product-fees-api
    """

    @sp_endpoint('/products/fees/v0/listings/{}/feesEstimate', method='POST')
    def get_product_fees_estimate_for_sku(self, seller_sku, price: float, shipping_price=None, currency='USD',
                                          is_fba=False, points: dict = None, marketplace_id: str = None,
                                          optional_fulfillment_program: str = None, force_safe_sku: bool = True,
                                          **kwargs) -> ApiResponse:
        """
        get_product_fees_estimate_for_sku(self, seller_sku, price: float, shipping_price=None, currency='USD', is_fba=False, points: dict = dict, **kwargs) -> ApiResponse

        Returns fees for sku

        Examples:
            literal blocks::

                ProductFees().get_product_fees_estimate_for_sku("UmaS1", 10, currency='USD', shipping_price=10, is_fba=False,
                                                          points={
                                                              "PointsNumber": 0,
                                                              "PointsMonetaryValue": {
                                                                  "CurrencyCode": "USD",
                                                                  "Amount": 0
                                                              }
                                                          })

        Args:
            seller_sku:
            price:
            shipping_price:
            currency:
            is_fba:
            points:
            marketplace_id: str | Defaults to self.marketplace_id
            optional_fulfillment_program:
            force_safe_sku: bool | Force user SKU quote
            **kwargs:

        Returns:
            ApiResponse:

        """

        if force_safe_sku:
            #handle `forward slash` issue in SKU
            seller_sku = quote_plus(seller_sku)

        kwargs.update(self._create_body(price, shipping_price, currency, is_fba, seller_sku, points, marketplace_id, optional_fulfillment_program))
        return self._request(fill_query_params(kwargs.pop('path'), seller_sku), data=kwargs)

    @sp_endpoint('/products/fees/v0/items/{}/feesEstimate', method='POST')
    def get_product_fees_estimate_for_asin(self, asin, price: float, currency='USD', shipping_price=None, is_fba=False,
                                           points: dict = None, marketplace_id: str = None,
                                           optional_fulfillment_program: str = None,
                                           **kwargs) -> ApiResponse:
        """
        get_product_fees_estimate_for_asin(self, asin, price: float, currency='USD', shipping_price=None, is_fba=False,  points: dict = dict, **kwargs) -> ApiResponse

        Returns fees for asin

        Examples:
            literal blocks::

                ProductFees().get_product_fees_estimate_for_asin("UmaS1", 10, currency='USD', shipping_price=10, is_fba=False,
                                                           points={
                                                               "PointsNumber": 0,
                                                               "PointsMonetaryValue": {
                                                                   "CurrencyCode": "USD",
                                                                   "Amount": 0
                                                               }
                                                           })

        Args:
            asin:
            price:
            currency:
            shipping_price:
            is_fba:
            points:
            marketplace_id: str | Defaults to self.marketplace_id
            optional_fulfillment_program:
            **kwargs:

        Returns:
            ApiResponse:

        """
        kwargs.update(self._create_body(price, shipping_price, currency, is_fba, asin, points, marketplace_id, optional_fulfillment_program))
        return self._request(fill_query_params(kwargs.pop('path'), asin), data=kwargs)


    def get_product_fees_estimate(self, estimate_requests: List[dict]) -> ApiResponse:
        """
        get_product_fees_estimate(self, estimate_requests: List[dict]) -> ApiResponse

        Return fees for multiple products

        Examples:
            literal blocks::

                ProductFees().get_product_fees_estimate(
                        [
                            dict(id_type='ASIN', id_value='B012345678', price=100),
                            dict(id_type='ASIN', id_value='B012345678', price=50, is_fba=True),
                        ]
                    )


        Args:
            estimate_requests: list of dict where the allowed keys are :
                id_type: str | ASIN or SellerSKU
                id_value: str
                price:
                currency:
                shipping_price:
                is_fba:
                points:
                marketplace_id: str | Defaults to self.marketplace_id
                optional_fulfillment_program:
        """
        data = [
            dict(
                **self._create_body(**er)
            )
            for er in estimate_requests
        ]
        return self._request('/products/fees/v0/feesEstimate', data=data, params=dict(method='POST'), wrap_list=True)


    def _create_body(self, price, shipping_price=None, currency='USD', is_fba=False, identifier=None, points: dict = None,
                     marketplace_id: str = None, optional_fulfillment_program: str=None, id_type=None, id_value=None):
        """
        Create request body

        Args:
            price:
            shipping_price:
            currency:
            is_fba:
            identifier:
            points:

        Returns:

        """
        body = {
            'FeesEstimateRequest': {
                'Identifier': identifier or str(price),
                'PriceToEstimateFees': {
                    'ListingPrice': {
                        'Amount': price,
                        'CurrencyCode': currency
                    },
                    'Shipping': {
                        'Amount': shipping_price,
                        'CurrencyCode': currency
                    } if shipping_price else None,
                    'Points': points or None
                },
                'IsAmazonFulfilled': is_fba,
                'OptionalFulfillmentProgram': optional_fulfillment_program if is_fba is True and optional_fulfillment_program else None,
                'MarketplaceId': marketplace_id or self.marketplace_id
            }
        }

        if id_type and id_value:
            body["IdType"] = id_type
            body["IdValue"] = id_value

        return body


    def _add_marketplaces(self, data):
        # MarketplaceID is a property of the body's FeesEstimateRequest for this section, and does
        # not need to be added. Additionally, Client._add_marketplaces will fail as it assumes
        # data is a dict, which is not the case for get_product_fees_estimate.
        pass
