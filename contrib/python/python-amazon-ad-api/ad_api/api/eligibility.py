from ad_api.base import Client, sp_endpoint, ApiResponse, Utils
import json
import logging


class Eligibility(Client):
    r""" """

    @sp_endpoint('/eligibility/product/list', method='POST')
    def get_eligibility(self, **kwargs) -> ApiResponse:
        r"""

        get_eligibility(body: (dict, str)) -> ApiResponse

        Gets a list of advertising eligibility objects for a set of products. Requests are permitted only for products sold by the merchant associated with the profile. Note that the request object is a list of ASINs, but multiple SKUs are returned if there is more than one SKU associated with an ASIN. If a product is not eligible for advertising, the response includes an object describing the reasons for ineligibility.

        body: | REQUIRED

            '**adType**': *string*, {'description': 'Set to 'sp' to check product eligibility for Sponsored Products advertisements. Set to 'sb' to check product eligibility for Sponsored Brands advertisements. default: sp. [ sp, sb ]'}

            '**productDetailsList**': *dict*, {'asin*': 'An Amazon product identifier.', 'sku': 'A seller product identifier'}

            '**locale**': *string*, {'description': 'Set to the locale string in the table below to specify the language in which the response is returned.'}

        Returns:

            ApiResponse

        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/eligibility/product/list', method='POST')
    def get_eligibility_assistant(self, asin_list: list, sku_list: list = None, ad_type: str = "sp", locale: str = "en-GB", **kwargs) -> ApiResponse:
        r"""

        get_eligibility_assistant(asin_list: list, sku_list: list = None, ad_type: str = "sp", locale:str = "en-GB") -> ApiResponse

        Gets a list of advertising eligibility objects for a set of products. Requests are permitted only for products sold by the merchant associated with the profile. Note that the request object is a list of ASINs, but multiple SKUs are returned if there is more than one SKU associated with an ASIN. If a product is not eligible for advertising, the response includes an object describing the reasons for ineligibility.

        | **asin_list**: 'A list of ASIN: An Amazon product identifier.'
        | **sku_list**: 'A list of SKU: A seller product identifier'
        | **adType**: *string*, {'description': 'Set to 'sp' to check product eligibility for Sponsored Products advertisements. Set to 'sb' to check product eligibility for Sponsored Brands advertisements. default: sp. [ sp, sb ]'}
        | **locale**': *string*, {'description': 'Set to the locale string in the table below to specify the language in which the response is returned. default: en_GB'}

        Returns:

            ApiResponse

        """

        asin_dict = []

        required = {}
        required.update({'adType': ad_type})

        if len(asin_list) > 0 and isinstance(sku_list, str) or sku_list is None:
            for asin in asin_list:
                asin_dict.append({'asin': asin})

        if sku_list is not None and isinstance(sku_list, list):
            if len(asin_list) == len(sku_list):
                for i in range(len(asin_list)):
                    asin_dict.append({'asin': asin_list[i], 'sku': sku_list[i]})
            else:
                logging.error("No coinciden las listas")

        required.update({'productDetailsList': asin_dict})
        required.update({'locale': locale})

        data_str = json.dumps(required)
        return self._request(kwargs.pop('path'), data=data_str, params=kwargs)
