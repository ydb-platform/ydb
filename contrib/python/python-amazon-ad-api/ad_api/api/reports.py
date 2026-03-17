from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Reports(Client):
    """Sponsored Products Reports Version 3

    Documentation: https://advertising.amazon.com/API/docs/en-us/offline-report-prod-3p

    Creates a report request. Use this operation to request the creation of a new report for Amazon Advertising Products. Use adProduct to specify the Advertising Product of the report.
    """

    @sp_endpoint('/reporting/reports', method='POST')
    def post_report(self, **kwargs) -> ApiResponse:
        r"""
        Requests a Sponsored Products report.

        Request the creation of a performance report for all entities of a single type which have performance data to report. Record types can be one of campaigns, adGroups, keywords, productAds, asins, and targets. Note that for asin reports, the report currently can not include metrics associated with both keywords and targets. If the targetingId value is set in the request, the report filters on targets and does not return sales associated with keywords. If the targetingId value is not set in the request, the report filters on keywords and does not return sales associated with targets. Therefore, the default behavior filters the report on keywords. Also note that if both keywordId and targetingId values are passed, the report filters on targets only and does not return keywords.

        Request body
            | **name** (string): [optional] The name of the report.
            | **startDate** (string): [required] The maximum lookback window supported depends on the selection of reportTypeId. Most report types support 95 days as lookback window. YYYY-MM-DD format.
            | **endDate**  (string): [required] The maximum lookback window supported depends on the selection of reportTypeId. Most report types support 95 days as lookback window. YYYY-MM-DD format.
            | **configuration** (AsyncReportConfigurationAsyncReportConfiguration): [required]
                | **adProduct** (string): [required] Enum: The advertising product such as SPONSORED_PRODUCTS or SPONSORED_BRANDS.
                | **columns** (list > string): [required] The list of columns to be used for report. The availability of columns depends on the selection of reportTypeId. This list cannot be null or empty.
                | **reportTypeId** (string): [required] The identifier of the Report Type to be generated.
                | **format** (string): [required] Enum: The report file format. [GZIP_JSON]
                | **groupBy** (list > string): [required] This field determines the aggregation level of the report data and also makes additional fields available for selection. This field cannot be null or empty.
                | **filters** (AsyncReportFilterAsyncReportFilter): [optional]
                    | **field** (string): The field name of the filter.
                    | **values** (list > string): The values to be filtered by.
                | **timeUnit** (string): [required] Enum: The aggregation level of report data. If the timeUnit is set to SUMMARY, the report data is aggregated at the time period specified. The availability of time unit breakdowns depends on the selection of reportTypeId. [ SUMMARY, DAILY ]

        Returns:
            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)

    @sp_endpoint('/reporting/reports/{}', method='GET')
    def get_report(self, reportId, **kwargs) -> ApiResponse:
        r"""
        Gets a generation status of a report by id. Uses the reportId value from the response of previously requested report via POST /reporting/reports operation. When status is set to COMPLETED, the report will be available to be downloaded at url.
        Report generation can take as long as 3 hours. Repeated calls to check report status may generate a 429 response, indicating that your requests have been throttled. To retrieve reports programmatically, your application logic should institute a delay between requests.

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
        return self._download(self, params=kwargs, headers={'User-Agent': self.user_agent})

    @sp_endpoint('/reporting/reports/{}', method='DELETE')
    def delete_report(self, reportId, **kwargs) -> ApiResponse:
        r"""
        Deletes a report by id. Use this operation to cancel a report in a PENDING status.

        Keyword Args
            | path **reportId** (number): The report identifier. [required]

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), reportId), params=kwargs)
