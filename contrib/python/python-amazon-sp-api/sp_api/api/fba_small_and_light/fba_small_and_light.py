import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, deprecated


class FbaSmallAndLight(Client):
    """
    FbaSmallAndLight SP-API Client
    :link: 

    The Selling Partner API for FBA Small and Light lets you help sellers manage their listings in the Small and Light program. The program reduces the cost of fulfilling orders for small and lightweight FBA inventory. You can enroll or remove items from the program and check item eligibility and enrollment status. You can also preview the estimated program fees charged to a seller for items sold while enrolled in the program.
    """

    @deprecated
    def get_small_and_light_enrollment_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse:
        return self.get_small_and_light_enrollment_by_seller_sku(sellerSKU, **kwargs)

    @sp_endpoint('/fba/smallAndLight/v1/enrollments/{}', method='GET')
    def get_small_and_light_enrollment_by_seller_sku(self, seller_sku, **kwargs) -> ApiResponse:
        """
        get_small_and_light_enrollment_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse

        Returns the Small and Light enrollment status for the item indicated by the specified seller SKU in the specified marketplace.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            seller_sku:string | * REQUIRED The seller SKU that identifies the item.
            key marketplaceIds:array | * REQUIRED The marketplace for which the enrollment status is retrieved. Note: Accepts a single marketplace only.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), seller_sku), params=kwargs)

    @deprecated
    def put_small_and_light_enrollment_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse:
        return self.put_small_and_light_enrollment_by_seller_sku(sellerSKU, **kwargs)

    @sp_endpoint('/fba/smallAndLight/v1/enrollments/{}', method='PUT')
    def put_small_and_light_enrollment_by_seller_sku(self, seller_sku, **kwargs) -> ApiResponse:
        """
        put_small_and_light_enrollment_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse

        Enrolls the item indicated by the specified seller SKU in the Small and Light program in the specified marketplace. If the item is not eligible, the ineligibility reasons are returned.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            seller_sku:string | * REQUIRED The seller SKU that identifies the item.
            key marketplaceIds:array | * REQUIRED The marketplace in which to enroll the item. Note: Accepts a single marketplace only.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), seller_sku),
                             params={'marketplaceIds': kwargs.get('marketplaceIds', self.marketplace_id)})

    @deprecated
    def delete_small_and_light_enrollment_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse:
        return self.delete_small_and_light_enrollment_by_seller_sku(sellerSKU, **kwargs)

    @sp_endpoint('/fba/smallAndLight/v1/enrollments/{}', method='DELETE')
    def delete_small_and_light_enrollment_by_seller_sku(self, seller_sku, **kwargs) -> ApiResponse:
        """
        delete_small_and_light_enrollment_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse

        Removes the item indicated by the specified seller SKU from the Small and Light program in the specified marketplace. If the item is not eligible for disenrollment, the ineligibility reasons are returned.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            seller_sku:string | * REQUIRED The seller SKU that identifies the item.
            key marketplaceIds:array | * REQUIRED The marketplace in which to remove the item from the Small and Light program. Note: Accepts a single marketplace only.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), seller_sku), data=kwargs)

    @deprecated
    def get_small_and_light_eligibility_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse:
        return self.get_small_and_light_eligibility_by_seller_sku(sellerSKU, **kwargs)

    @sp_endpoint('/fba/smallAndLight/v1/eligibilities/{}', method='GET')
    def get_small_and_light_eligibility_by_seller_sku(self, seller_sku, **kwargs) -> ApiResponse:
        """
        get_small_and_light_eligibility_by_seller_s_k_u(self, sellerSKU, **kwargs) -> ApiResponse

        Returns the Small and Light program eligibility status of the item indicated by the specified seller SKU in the specified marketplace. If the item is not eligible, the ineligibility reasons are returned.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       10
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            seller_sku:string | * REQUIRED The seller SKU that identifies the item.
            key marketplaceIds:array | * REQUIRED The marketplace for which the eligibility status is retrieved. NOTE: Accepts a single marketplace only.

        Returns:
            ApiResponse:
        """

        return self._request(fill_query_params(kwargs.pop('path'), seller_sku), params=kwargs)

    @sp_endpoint('/fba/smallAndLight/v1/feePreviews', method='POST')
    def get_small_and_light_fee_preview(self, **kwargs) -> ApiResponse:
        """
        get_small_and_light_fee_preview(self, **kwargs) -> ApiResponse

        Returns the Small and Light fee estimates for the specified items. You must include a marketplaceId parameter to retrieve the proper fee estimates for items to be sold in that marketplace. The ordering of items in the response will mirror the order of the items in the request. Duplicate ASIN/price combinations are removed.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       3
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            body:  REQUIRED
            {
              "marketplaceId": "string",
              "items": [
                {
                  "asin": "string",
                  "price": {
                    "currencyCode": "string",
                    "amount": 0
                  }
                }
              ]
            }

        Returns:
            ApiResponse:
        """

        return self._request(kwargs.pop('path'), data=kwargs)
