from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class RankedKeywordsRecommendations(Client):
    """
    Specification: https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/SponsoredProducts_prod_3p.json
    """

    @sp_endpoint('/sp/targets/keywords/recommendations', method='POST')
    def list_ranked_keywords_recommendations(self, version: int = 3, **kwargs) -> ApiResponse:
        r"""
        Get keyword recommendations

        The POST /sp/targets/keywords/recommendations endpoint returns recommended keyword targets given either A) a list of ad ASINs or B) a campaign ID and ad group ID. Please use the recommendationType field to specify if you want to use option A or option B. This endpoint will also return recommended bids along with each recommendation keyword target.

        Ranking
        The keyword recommendations will be ranked in descending order of clicks or impressions, depending on the sortDimension field provided by the user. You may also input your own keyword targets to be ranked alongside the keyword recommendations by using the targets array.

        Localization
        Use the locale field to get keywords in your specified locale. Supported marketplace to locale mappings can be found at the POST /keywords/localize endpoint.

        Version 5.0

        New Features

        Version 5.0 utilizes the new theme-based bid recommendations, which can be retrieved at the endpoint /sp/targets/bid/recommendations, to return improved bid recommendations for each keyword. Theme-based bid recommendations provide \"themes\" and \"impact metrics\" along with each bid suggestion to help you choose the right bid for your keyword target.

        **Themes**

        We now may return multiple bid suggestions for each keyword target. Each suggestion will have a theme to express the business objective of the bid. Available themes are:

        - CONVERSION_OPPORTUNITIES - The default theme which aims to maximize number of conversions.

        - SPECIAL_DAYS - A theme available during high sales events such as Prime Day, to anticipate an increase in sales and competition.

        **Impact Metrics**

        We have added impact metrics which provide insight on the number of clicks and conversions you will receive for targeting a keyword at a certain bid.

        **Bidding Strategy**

        You may now specify your bidding strategy in the KEYWORDS_BY_ASINS request to get bid suggestions tailored to your bidding strategy. For KEYWORDS_BY_ADGROUP requests, you will not specify a bidding strategy, because the bidding strategy of the ad group is used. The three bidding strategies are:

        - LEGACY_FOR_SALES - Dynamic bids (down only)
        - AUTO_FOR_SALES - Dynamic bids (up and down)
        - MANUAL - Fixed bids

        Availability

        Version 5.0 is only available in the following marketplaces: US, CA, UK, DE, FR, ES, IN, JP.

        Request Body (oneOf)
            | This request type is used to retrieve recommended keyword targets for an existing ad group. Set the recommendationType to KEYWORD_FOR_ADGROUP to use this request type.

            | RankedKeywordTargetsForAdGroupRequest {
            | **maxRecommendations** (number): The max size of recommended target. Set it to 0 if you only want to rank user-defined keywords. default: 200 maximum: 200 minimum: 0
            | **sortDimension** (string): The ranking metric value. Supported values: CLICKS, CONVERSIONS, DEFAULT. DEFAULT will be applied if no value passed in. Enum: ['CLICKS', 'CONVERSIONS', 'DEFAULT']
            | **locale** (string): Translations are for readability and do not affect the targeting of ads. Supported marketplace to locale mappings can be found at the <a href='https://advertising.amazon.com/API/docs/en-us/localization/#/Keyword%20Localization'>POST /keywords/localize</a> endpoint. Note: Translations will be null if locale is unsupported. Enum: ['ar_EG', 'de_DE', 'en_AE', 'en_AU', 'en_CA', 'en_GB', 'en_IN', 'en_SA', 'en_SG', 'en_US', 'es_ES', 'es_MX', 'fr_FR', 'it_IT', 'ja_JP', 'nl_NL', 'pl_PL', 'pt_BR', 'sv_SE', 'tr_TR', 'zh_CN']
            | **targets** [
            | minItems: 0. maxItems: 100. A list of targets that need to be ranked.
            |   {
            |    **matchType** (string): Keyword match type. The default value will be BROAD. Enum: ['BROAD', 'EXACT', 'PHRASE']
            |    **keyword** (string): The keyword value
            |    **bid** number (double): The bid value for the keyword. The default value will be the suggested bid.
            |    **userSelectedKeyword** (boolean): Flag that tells if keyword was selected by the user or was recommended by KRS
            |   }
            | ]
            | **campaignId**\*(string): The identifier of the campaign
            | **recommendationType**\*(string): The recommendationType to retrieve recommended keyword targets for an existing ad group. Enum: ['KEYWORDS_FOR_ADGROUP']
            | **bidsEnabled**(boolean): Set this parameter to false if you do not want to retrieve bid suggestions for your keyword targets. Defaults to true. default: true
            | **dGroupId**\*(string): The identifier of the ad group
            | }

            | This request type is used to retrieve recommended keyword targets for ASINs. Set the recommendationType to KEYWORD_FOR_ASINS to use this request type.

            | RankedKeywordTargetsForAsinsRequest {
            | **maxRecommendations** (number): The max size of recommended target. Set it to 0 if you only want to rank user-defined keywords. default: 200 maximum: 200 minimum: 0
            | **sortDimension** (string): The ranking metric value. Supported values: CLICKS, CONVERSIONS, DEFAULT. DEFAULT will be applied if no value passed in. Enum: ['CLICKS', 'CONVERSIONS', 'DEFAULT']
            | **locale** (string): Translations are for readability and do not affect the targeting of ads. Supported marketplace to locale mappings can be found at the <a href='https://advertising.amazon.com/API/docs/en-us/localization/#/Keyword%20Localization'>POST /keywords/localize</a> endpoint. Note: Translations will be null if locale is unsupported. Enum: ['ar_EG', 'de_DE', 'en_AE', 'en_AU', 'en_CA', 'en_GB', 'en_IN', 'en_SA', 'en_SG', 'en_US', 'es_ES', 'es_MX', 'fr_FR', 'it_IT', 'ja_JP', 'nl_NL', 'pl_PL', 'pt_BR', 'sv_SE', 'tr_TR', 'zh_CN']
            | **targets** [
            | minItems: 0. maxItems: 100. A list of targets that need to be ranked.
            |   {
            |    **matchType** (string): Keyword match type. The default value will be BROAD. Enum: ['BROAD', 'EXACT', 'PHRASE']
            |    **keyword** (string): The keyword value
            |    **bid** number (double): The bid value for the keyword. The default value will be the suggested bid.
            |    **userSelectedKeyword** (boolean): Flag that tells if keyword was selected by the user or was recommended by KRS
            |   }
            | **asins**\* (array) maxItems: 50. An array list of Asin.
            | **biddingStrategy** (string) The bid recommendations returned will depend on the bidding strategy. LEGACY_FOR_SALES - Dynamic Bids (Down only) AUTO_FOR_SALES - Dynamic Bids (Up or down) MANUAL - Fixed Bids. Enum: [ LEGACY_FOR_SALES, AUTO_FOR_SALES, MANUAL, RULE_BASED ] default: LEGACY_FOR_SALES
            | **recommendationType**\* (string): The recommendationType to retrieve recommended keyword targets for a list of ASINs. Enum: ['KEYWORDS_FOR_ASINS']
            | **bidsEnabled**(boolean): Set this parameter to false if you do not want to retrieve bid suggestions for your keyword targets. Defaults to true. default: true
            | }

        Version 4.0

        New Features

        Version 4.0 allows users to retrieve recommended keyword targets which are sorted in descending order of clicks or conversions. The default sort dimension, if not specified, ranks recommendations by our interal ranking mechanism. We have also have added search term metrics. **Search term impression share** indicates the percentage share of all ad-attributed impressions you received on that keyword in the last 30 days. This metric helps advertisers identify potential opportunities based on their share on relevant keywords. **Search term impression rank** indicates your ranking among all advertisers for the keyword by ad impressions in a marketplace. It tells an advertiser how many advertisers had higher share of ad impressions. Search term information is only available for keywords the advertiser targeted with ad impressions.

        Availability

        Version 4.0 is available in all marketplaces.


        Request Body (oneOf)
            | This request type is used to retrieve recommended keyword targets for an existing ad group. Set the recommendationType to KEYWORD_FOR_ADGROUP to use this request type.

            | AdGroupKeywordTargetRankRecommendationRequest {
            | **maxRecommendations** (number): The max size of recommended target. Set it to 0 if you only want to rank user-defined keywords. default: 200 maximum: 200 minimum: 0
            | **sortDimension** (string): The ranking metric value. Supported values: CLICKS, CONVERSIONS, DEFAULT. DEFAULT will be applied if no value passed in. Enum: ['CLICKS', 'CONVERSIONS', 'DEFAULT']
            | **locale** (string): Translations are for readability and do not affect the targeting of ads. Supported marketplace to locale mappings can be found at the <a href='https://advertising.amazon.com/API/docs/en-us/localization/#/Keyword%20Localization'>POST /keywords/localize</a> endpoint. Note: Translations will be null if locale is unsupported. Enum: ['ar_EG', 'de_DE', 'en_AE', 'en_AU', 'en_CA', 'en_GB', 'en_IN', 'en_SA', 'en_SG', 'en_US', 'es_ES', 'es_MX', 'fr_FR', 'it_IT', 'ja_JP', 'nl_NL', 'pl_PL', 'pt_BR', 'sv_SE', 'tr_TR', 'zh_CN']
            | **targets** [
            | minItems: 0. maxItems: 100. A list of targets that need to be ranked.
            |   {
            |    **matchType** (string): Keyword match type. The default value will be BROAD. Enum: ['BROAD', 'EXACT', 'PHRASE']
            |    **keyword** (string): The keyword value
            |    **bid** number (double): The bid value for the keyword. The default value will be the suggested bid.
            |    **userSelectedKeyword** (boolean): Flag that tells if keyword was selected by the user or was recommended by KRS
            |   }
            | ]
            | **campaignId**\*(string): The identifier of the campaign
            | **recommendationType**\*(string): The recommendationType to retrieve recommended keyword targets for an existing ad group. Enum: ['KEYWORDS_FOR_ADGROUP']
            | **dGroupId**\*(string): The identifier of the ad group
            | }

            | This request type is used to retrieve recommended keyword targets for ASINs. Set the recommendationType to KEYWORD_FOR_ASINS to use this request type.

            | AsinsKeywordTargetRankRecommendationRequest {
            | **maxRecommendations** (number): The max size of recommended target. Set it to 0 if you only want to rank user-defined keywords. default: 200 maximum: 200 minimum: 0
            | **sortDimension** (string): The ranking metric value. Supported values: CLICKS, CONVERSIONS, DEFAULT. DEFAULT will be applied if no value passed in. Enum: ['CLICKS', 'CONVERSIONS', 'DEFAULT']
            | **locale** (string): Translations are for readability and do not affect the targeting of ads. Supported marketplace to locale mappings can be found at the <a href='https://advertising.amazon.com/API/docs/en-us/localization/#/Keyword%20Localization'>POST /keywords/localize</a> endpoint. Note: Translations will be null if locale is unsupported. Enum: ['ar_EG', 'de_DE', 'en_AE', 'en_AU', 'en_CA', 'en_GB', 'en_IN', 'en_SA', 'en_SG', 'en_US', 'es_ES', 'es_MX', 'fr_FR', 'it_IT', 'ja_JP', 'nl_NL', 'pl_PL', 'pt_BR', 'sv_SE', 'tr_TR', 'zh_CN']
            | **targets** [
            | minItems: 0. maxItems: 100. A list of targets that need to be ranked.
            |   {
            |    **matchType** (string): Keyword match type. The default value will be BROAD. Enum: ['BROAD', 'EXACT', 'PHRASE']
            |    **keyword** (string): The keyword value
            |    **bid** number (double): The bid value for the keyword. The default value will be the suggested bid.
            |    **userSelectedKeyword** (boolean): Flag that tells if keyword was selected by the user or was recommended by KRS
            |   }
            | **asins**\* (array) maxItems: 50. An array list of Asin.
            | **recommendationType**\* (string): The recommendationType to retrieve recommended keyword targets for a list of ASINs. Enum: ['KEYWORDS_FOR_ASINS']
            | }
        """

        contentType = 'application/vnd.spkeywordsrecommendation.v' + str(version) + "+json"
        headers = {'Content-Type': contentType}
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs, headers=headers)
