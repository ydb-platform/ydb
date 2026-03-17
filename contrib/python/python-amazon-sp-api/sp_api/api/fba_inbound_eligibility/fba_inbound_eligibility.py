import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, IneligibilityReasonList, ApiResponse


class FbaInboundEligibility(Client):
    """
    FbaInboundEligibility SP-API Client
    :link: 

    With the FBA Inbound Eligibility API, you can build applications that let sellers get eligibility previews for items before shipping them to Amazon's fulfillment centers. With this API you can find out if an item is eligible for inbound shipment to Amazon's fulfillment centers in a specific marketplace. You can also find out if an item is eligible for using the manufacturer barcode for FBA inventory tracking. Sellers can use this information to inform their decisions about which items to ship Amazon's fulfillment centers.
    """


    @sp_endpoint('/fba/inbound/v1/eligibility/itemPreview', method='GET')
    def get_item_eligibility_preview(self, **kwargs) -> ApiResponse:
        """
        get_item_eligibility_preview(self, **kwargs) -> ApiResponse

        This operation gets an eligibility preview for an item that you specify. You can specify the type of eligibility preview that you want (INBOUND or COMMINGLING). For INBOUND previews, you can specify the marketplace in which you want to determine the item's eligibility.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceIds:array |  The identifier for the marketplace in which you want to determine eligibility. Required only when program=INBOUND.
            key asin:string | * REQUIRED The ASIN of the item for which you want an eligibility preview.
            key program:string | * REQUIRED The program that you want to check eligibility against.
        

        Returns:
            ApiResponse:
        """
        return self._request(kwargs.pop('path'),  params=kwargs)

    @sp_endpoint('/fba/inbound/v1/eligibility/itemPreview', method='GET')
    def get_item_eligibility_preview_extended(self, **kwargs) -> ApiResponse:
        """
        get_item_eligibility_preview_extended(self, **kwargs) -> ApiResponse

        This operation gets an eligibility preview for an item that you specify. You can specify the type of eligibility preview that you want (INBOUND or COMMINGLING). For INBOUND previews, you can specify the marketplace in which you want to determine the item's eligibility.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       1
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            key marketplaceIds:array |  The identifier for the marketplace in which you want to determine eligibility. Required only when program=INBOUND.
            key asin:string | * REQUIRED The ASIN of the item for which you want an eligibility preview.
            key program:string | * REQUIRED The program that you want to check eligibility against.


        Returns:
            ApiResponse:
        """

        api_response = self._request(kwargs.pop('path'), params=kwargs)

        if api_response.payload.get("ineligibilityReasonList") and api_response.payload.get("isEligibleForProgram") is False:
            ineligibility_list = api_response.payload.get("ineligibilityReasonList")
            errors = []
            for ineligibility_option in ineligibility_list:
                try:
                    errors.append(ineligibility_option + ": " + IneligibilityReasonList[ineligibility_option].value)
                except KeyError as error:
                    errors.append(error)
            api_response.payload.update({
                "ineligibilityReasonListMessage": errors
            })

        return api_response
