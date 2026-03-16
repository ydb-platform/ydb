from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Reports(Client):
    """Sponsored Products Reports

    Documentation: https://advertising.amazon.com/API/docs/en-us/sponsored-products/2-0/openapi#/Reports

    Use the Amazon Advertising API for Sponsored Products for campaign, ad group, keyword, negative keyword, and product ad management operations. For more information about Sponsored Products, see the Sponsored Products Support Center. For onboarding information, see the account setup topic.
    """

    @sp_endpoint('/v2/sp/{}/report', method='POST')
    @Utils.deprecated
    def post_report(self, recordType, **kwargs) -> ApiResponse:
        r"""
        Requests a Sponsored Products report.

        Request the creation of a performance report for all entities of a single type which have performance data to report. Record types can be one of campaigns, adGroups, keywords, productAds, asins, and targets. Note that for asin reports, the report currently can not include metrics associated with both keywords and targets. If the targetingId value is set in the request, the report filters on targets and does not return sales associated with keywords. If the targetingId value is not set in the request, the report filters on keywords and does not return sales associated with targets. Therefore, the default behavior filters the report on keywords. Also note that if both keywordId and targetingId values are passed, the report filters on targets only and does not return keywords.

        Keyword Args
            | path **recordType** (integer): The type of entity for which the report should be generated. Available values : campaigns, adGroups, keywords, productAds, asins, targets [required]

        Request body
            | **stateFilter** (string): [optional] Filters the response to include reports with state set to one of the values in the comma-delimited list. Note that this filter is only valid for reports of the following type and segment. Asins and targets report types are not supported. Enum [ enabled, paused, archived ].
            | **campaignType** (list > string): [required] Enum: The type of campaign. Only required for asins report - don't use with other report types. [sponsoredProducts]
            | **segment** (string) Dimension on which the report is segmented. Note that Search-terms report for auto-targeted campaigns created before 11/14/2018 can be accessed from the /v2/sp/keywords/report resource. Search-terms report for auto-targeted campaigns generated on-and-after 11/14/2018 can be accessed from the /v2/sp/targets/report resource. Also, keyword search terms reports only return search terms that have generated at least one click or one sale. Enum [ query, placement ].
            | **reportDate** (string): [optional] The date for which to retrieve the performance report in YYYYMMDD format. The time zone is specified by the profile used to request the report. If this date is today, then the performance report may contain partial information. Reports are not available for data older than 60 days. For details on data latency, see the Service Guarantees in the developer notes section.
            | **metrics** (string) [optional] A comma-separated list of the metrics to be included in the report. The following tables summarize report metrics which can be requested via the reports interface. Different report types can use different metrics. Note that ASIN reports only return data for either keywords or targets, but not both.

        Returns:
            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), recordType), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/reports/{}', method='GET')
    def get_report(self, reportId, **kwargs) -> ApiResponse:
        r"""
        Gets a previously requested report specified by identifier.

        Keyword Args
            | path **reportId** (number): The report identifier. [required]

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), reportId), params=kwargs)

    def download_report(self, **kwargs) -> ApiResponse:
        r"""
        Downloads the report previously get report specified by location (this is not part of the official Amazon Advertising API, is a helper method to download the report). Take in mind that a direct download of location returned in get_report will return 401 - Unauthorized.

        kwarg parameter **file** if not provided will take the default amazon name from path download (add a path with slash / if you want a specific folder, do not add extension as the return will provide the right extension based on format choosed if needed)

        kwarg parameter **format** if not provided a format will return a url to download the report (this url has a expiration time)

        Keyword Args
            | **url** (string): The location obatined from get_report [required]
            | **file** (string): The path to save the file if mode is download json, zip or gzip. [optional]
            | **format** (string): The mode to download the report: data (list), raw, url, json, zip, gzip. Default (url) [optional]

        Returns:
            ApiResponse
        """
        return self._download(self, params=kwargs)
