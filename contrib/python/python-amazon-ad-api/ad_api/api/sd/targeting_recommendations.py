from ad_api.base import Client, sp_endpoint, ApiResponse


class TargetsRecommendations(Client):
    """Amazon Advertising API for Sponsored Display

    Documentation: https://advertising.amazon.com/API/docs/en-us/sponsored-display/3-0/openapi#/Targeting%20Recommendations

    This API enables programmatic access for campaign creation, management, and reporting for Sponsored Display campaigns. For more information on the functionality, see the `Sponsored Display Support Center <https://advertising.amazon.com/help#GTPPHE6RAWC2C4LZ>`_ . For API onboarding information, see the `account setup <https://advertising.amazon.com/API/docs/en-us/setting-up/account-setup>`_  topic.

    This specification is available for download from the `Advertising API developer portal <https://d3a0d0y2hgofx6.cloudfront.net/openapi/en-us/sponsored-display/3-0/openapi.yaml>`_.

    """

    @sp_endpoint('/sd/targets/recommendations', method='POST')
    def list_targets_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        Note that version application/vnd.sdtargetingrecommendations.v3.1+json is now supported.
        Provides a list of products to target based on the list of input ASINs. Allow 1 week for our systems to process data for any new ASINs listed on Amazon before using this service.
        Currently the API will return up to 100 recommended products and categories.
        The currently available tactic identifiers are:

        +-------------+--------------------+------------------------------------------------------------------------------------------------+
        | Tactic Name | Type               | Description                                                                                    |
        +=============+====================+================================================================================================+
        | T00020      | Product Targeting  | Products: Choose individual products to show your ads in placements related to those products. |
        +-------------+--------------------+------------------------------------------------------------------------------------------------+
        | T00030      | Audience Targeting | Audiences: Select individual audiences to show your ads                                        |
        +-------------+--------------------+------------------------------------------------------------------------------------------------+

        body: SDTargetingRecommendationsRequestV31 | REQUIRED {'description': 'Request for targeting recommendations.}'

            | '**tactic***': *SDTacticV31string*, {'description': 'The advertising tactic associated with the campaign.', 'Enum': '[ T00020, T00030 ]'}
            | '**products**': *SDGoalProduct*, {'description': 'A list of products for which to get targeting recommendations. minItems: 1, maxItems: 10000'}

                | '**SDGoalProduct***', {'description': 'A product an advertisers wants to advertise. Recommendations will be made for specified goal products.}
                | '**asin***': *SDASINstring*, REQUIRED {'description': 'Amazon Standard Identification Number. pattern: [a-zA-Z0-9]{10}. example: B00PN11UNW}

            | '**typeFilter**': *SDRecommendationTypeV31string*, {'description': 'Signifies a type of recommendation', 'Enum': '[ PRODUCT, CATEGORY ]'}

        Returns:

            ApiResponse


        """
        contentType = 'application/vnd.sdtargetingrecommendations.v3.1+json'
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs, headers=headers)
