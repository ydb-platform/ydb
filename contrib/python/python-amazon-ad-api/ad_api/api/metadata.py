from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class Metadata(Client):
    r""" """

    @sp_endpoint('/product/metadata', method='POST')
    def get_products_metadata(self, **kwargs) -> ApiResponse:
        r"""

        get_products_metadata(body: (dict, str)) -> ApiResponse

        Returns product metadata for the advertiser.

        body: | REQUIRED

            '**asins**': *list>string*, {'description': 'Specific asins to search for in the advertiser's inventory. Cannot use together with skus or searchStr input types.'}

            '**checkItemDetails**': *boolean*, {'description': 'Whether item details such as name, image, and price is required. default: false'}

            '**cursorToken**': *string*, {'description': 'Pagination token used for the suggested sort type'}

            '**adType**': *string*, {'description': 'Program type. Required if checks advertising eligibility. Enum: [ SP, SB, SD ]'}

            '**skus**': *list>string*, {'description': 'Specific skus to search for in the advertiser's inventory. Currently only support SP program type for sellers. Cannot use together with asins or searchStr input types'}

            '**checkEligibility**': *boolean*, {'description': 'Whether advertising eligibility info is required. default: false'}

            '**searchStr**': *string*, {'description': 'Specific string in the item title to search for in the advertiser's inventory. Case insensitive. Cannot use together with asins or skus input types'}

            '**pageIndex**': *integer($int32)*, {'description*': 'Index of the page to be returned'}

            '**sortOrder**': *string*, {'description': 'Sort order (has to be DESC for the suggested sort type). default: DESC. Enum [ ASC, DESC ]'}

            '**pageSize**': *integer($int32)*, {'description*': 'Number of items to be returned on this page index (max 100 for author)'}

            '**sortBy**': *string*, {'description': 'Sort option for the result. Currently only support SP program type for sellers. Enum [ SUGGESTED, CREATED_DATE ]'}



        Returns:

            ApiResponse

        """
        contentType = 'application/vnd.productmetadatarequest.v1+json'
        headers = {'Content-Type': contentType}
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)
