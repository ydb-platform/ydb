from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class Audiences(Client):
    """ """

    @sp_endpoint('/audiences/taxonomy/list', method='POST')
    def list_audiences_taxonomy(self, **kwargs) -> ApiResponse:
        r"""
        list_audiences_taxonomy(body: dict or str, **kwargs) -> ApiResponse:

        Returns a list of audience categories for a given category path

        Requires one of these permissions: ["advertiser_campaign_edit","advertiser_campaign_view"]

        Kwargs:

        **advertiserId**: *string* (query) The advertiser associated with the advertising account. This parameter is required for the DSP adType, but optional for the SD adType.

        **nextToken**: *string* (query) Token from a previous request. Use in conjunction with the maxResults parameter to control pagination of the returned array.

        **maxResults**: *integer* (query) Sets the maximum number of categories in the returned array. Use in conjunction with the nextToken parameter to control pagination. For example, supplying maxResults=20 with a previously returned token will fetch up to the next 20 items. In some cases, fewer items may be returned. Default value : 250

        **body**: *dict or str* (data) REQUIRED
        {

            "adType": **string** Enum [ DSP, SD ]

            "categoryPath":

                [

                **string**

                ]
        }

        Returns ApiResponse

        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/audiences/list', method='POST')
    def list_audiences(self, **kwargs) -> ApiResponse:
        r"""
        list_audiences(body: dict or str, **kwargs) -> ApiResponse:

        Returns a list of audience segments for an advertiser. The result set can be filtered by providing an array of Filter objects. Each item in the resulting set will match all specified filters.

        Requires one of these permissions: ["advertiser_campaign_edit","advertiser_campaign_view"]

        Kwargs:

        **advertiserId**: *string* (query) The advertiser associated with the advertising account. This parameter is required for the DSP adType, but optional for the SD adType.

        **nextToken**: *string* (query) Token from a previous request. Use in conjunction with the maxResults parameter to control pagination of the returned array.

        **maxResults**: *integer* (query) Sets the maximum number of categories in the returned array. Use in conjunction with the nextToken parameter to control pagination. For example, supplying maxResults=20 with a previously returned token will fetch up to the next 20 items. In some cases, fewer items may be returned. Default value : 10

        **body**: *dict or str* (data) REQUIRED
        {

            "adType": **string** Enum [ DSP, SD ]

            "filters": [

                {

                    "field": **string** Field to filter by. Supported enums are 'audienceName', 'category', 'categoryPath' and 'audienceId'. The 'category' enum returns all audiences under a high-level category, whereas the 'categoryPath' enum expects a path of nodes in the taxonomy tree and returns audiences attached directly to the node at the specified path.

                    "values": [

                        **string**

                    ]

                }

            ]

        }

        Returns ApiResponse

        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)
