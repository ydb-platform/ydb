from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class BrandSafety(Client):
    @sp_endpoint('/sd/brandSafety/deny', method='GET')
    def list_brand_safety(self, **kwargs) -> ApiResponse:
        r"""
        Gets an array of websites/apps that are on the advertiser's Brand Safety Deny List. It can take up to 15 minutes from the time a domain is added/deleted to the time it is reflected in the deny list.

            query **startIndex**:*integer* | Optional. Optional. Sets a cursor into the requested set of domains. Use in conjunction with the count parameter to control pagination of the returned array. 0-indexed record offset for the result set, defaults to 0.

            query **count**:*integer* | Optional. Sets the number of domain objects in the returned array. Use in conjunction with the startIndex parameter to control pagination. For example, to return the first 1000 domains set startIndex=0 and count=1000. To return the next 1000 domains, set startIndex=1000 and count=1000, and so on. Defaults to max page size(1000).

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/brandSafety/deny', method='POST')
    def post_brand_safety(self, **kwargs) -> ApiResponse:
        r"""
        Creates one or more domains to add to a Brand Safety Deny List. The Brand Safety Deny List is at the advertiser level. It can take up to 15 minutes from the time a domain is added to the time it is reflected in the deny list.

        Request Body
            | BrandSafetyPostRequest {
            | POST Request for Brand Safety
            | **domains**\* (array) minItems: 0 maxItems: 10000 [
            |   BrandSafetyDenyListDomain {
            |       **name**\* (string): The website or app identifier. This can be in the form of full domain (eg. 'example.com' or 'example.net'), or mobile app identifier (eg. 'com.example.app' for Android apps or '1234567890' for iOS apps). maxLength: 250
            |       **type**\* (BrandSafetyDenyListDomainType >> string): The domain type. Enum: [ WEBSITE, APP ]
            |       }
            |   ]
            | }
        Returns
            ApiResponse
        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)

    @sp_endpoint('/sd/brandSafety/deny', method='DELETE')
    def delete_brand_safety(self, **kwargs) -> ApiResponse:
        r"""
        Archives all of the domains in the Brand Safety Deny List. It can take several hours from the time a domain is deleted to the time it is reflected in the deny list. You can check the status of the delete request by calling GET /sd/brandSafety/{requestId}/status. If the status is "COMPLETED", you can call GET /sd/brandSafety/deny to validate that your deny list has been successfully deleted.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/brandSafety/{}/results', method='GET')
    def get_result_brand_safety_request(self, requestId, **kwargs) -> ApiResponse:
        r"""
        When a user adds domains to their Brand Safety Deny List, the request is processed asynchronously, and a requestId is provided to the user. This requestId can be used to view the request results for up to 90 days from when the request was submitted. The results provide the status of each domain in the given request. Request results may contain multiple pages. This endpoint will only be available once the request has completed processing. To see the status of the request you can call GET /sd/brandSafety/{requestId}/status. Note that this endpoint only lists the results of POST requests to /sd/brandSafety/deny - it does not reflect the results of DELETE requests.

            query **requestId**\*:*string* | The ID of the request previously submitted.

            query **startIndex**:*integer* | Optional. Sets a cursor into the requested set of domains. Use in conjunction with the count parameter to control pagination of the returned array. 0-indexed record offset for the result set, defaults to 0.

            query **count**:*integer* | Optional. Sets the number of domain objects in the returned array. Use in conjunction with the startIndex parameter to control pagination. For example, to return the first 1000 domains set startIndex=0 and count=1000. To return the next 1000 domains, set startIndex=1000 and count=1000, and so on. Defaults to max page size(1000).

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), requestId), params=kwargs)

    @sp_endpoint('/sd/brandSafety/{}/status', method='GET')
    def get_status_brand_safety_request(self, requestId, **kwargs) -> ApiResponse:
        r"""
        When a user modifies their Brand Safety Deny List, the request is processed asynchronously, and a requestId is provided to the user. This requestId can be used to check the status of the request for up to 90 days from when the request was submitted.

            query **requestId**\*:*string* | The ID of the request previously submitted.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), requestId), params=kwargs)

    @sp_endpoint('/sd/brandSafety/status', method='GET')
    def list_brand_safety_requests_history(self, **kwargs) -> ApiResponse:
        r"""
        List status of all Brand Safety List requests. The list will contain requests that were submitted in the past 90 days.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)
