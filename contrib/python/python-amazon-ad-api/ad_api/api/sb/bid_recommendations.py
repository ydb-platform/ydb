from ad_api.base import Client, sp_endpoint, ApiResponse


class BidRecommendations(Client):
    """
    Use the Amazon Advertising API for Sponsored Brands for campaign, ad group, keyword, negative keyword, drafts, Stores, landing pages, and Brands management operations. For more information about Sponsored Brands, see the Sponsored Brands Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/sb/recommendations/bids', method='POST')
    def get_bid_recommendations(self, **kwargs) -> ApiResponse:
        r"""
        Get a list of bid recommendation objects for a specified list of keywords or products.

        Request Body
            | '**campaignId**': *integer($int64)*, {'description': 'The identifier of the campaign for which bid recommendations are created.'}
            | '**targets**': *integer($int64)*, {'SBExpression': 'A name value pair that defines a targeting expression. The type field defines the predicate. The value field defines the value to match for the predicate.'}

                | '**type**': *string*, {'values': '[ asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs ]'}

                | '**value**': *string*, {'description': 'The text of the targeting expression. The - token defines a range. For example, 2-4 defines a range of 2, 3, and 4.'}

            | '**keywords**': *string*, {'description': 'SBBidRecommendationKeyword'}

                | '**matchType**': *string*, {'values': '[ broad, exact, phrase ]'}

                | '**keywordText**': *string*, {'description': 'The text of the keyword. Maximum of 10 words.'}

            | '**adFormat**': *integer($int64)*, {'values': '[ productCollection, video ]'}

        Returns:
            ApiResponse

        """

        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
