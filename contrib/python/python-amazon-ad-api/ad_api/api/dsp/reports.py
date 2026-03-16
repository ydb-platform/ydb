from ad_api.api.dsp.client import DspClient as Client
from ad_api.base import sp_endpoint, fill_query_params, ApiResponse


class Reports(Client):
    """Amazon DSP Reports

    Documentation: https://advertising.amazon.com/API/docs/en-us/dsp-reports-beta-3p/#/Reports

    Amazon DSP is a demand-side platform (DSP) that enables advertisers to programmatically buy display, video, and audio ads both on and off Amazon. Using Amazon's DSP, you can reach audiences across the web on both Amazon sites and apps as well as through our publishing partners and third-party exchanges. Amazon DSP is available to both advertisers who sell products on Amazon and those who do not.
    """

    @sp_endpoint('/accounts/{}/dsp/reports', method='POST')
    def post_report(self, dspAccountId, accept='application/vnd.dspcreatereports.v3+json', **kwargs) -> ApiResponse:
        r"""
        Request reports with performance metrics for DSP campaigns.

        Request creation of a report that includes metrics about your Amazon DSP campaigns. Specify the type of report and the metrics you'd like to include. Note that the value specified for the dimensions field affects the metrics included in the report. See the dimensions field description for more information.

        Keyword Args
            | path **dspAccountId** (string): Account Identifier you use to access the DSP. This is your DSP Entity ID if you have access to all DSP advertisers within that entity, or your DSP Advertiser ID if you only have access to a specific advertiser ID. [required]

        Request body
            | **advertiserIds** (list > string): [optional] List of advertisers specified by identifier to include in the report. This should not be present if accountId is advertiser.
            | **endDate** (string): [required] Date in yyyy-MM-dd format. The report contains only metrics generated on the specified date range between startDate and endDate. The maximum date range between startDate and endDate is 31 days. The endDate can be up to 90 days older from today.
            | **format** (string): [optional] Enum: The report file format. [JSON]
            | **orderIds** (list > string): [optional] List of orders specified by identifier to include in the report.
            | **metrics** (list > string): [optional] Specify a list of metrics field names to include in the report. For example: ["impressions", "clickThroughs", "CTR", "eCPC", "totalCost", "eCPM"]. If no metric field names are specified, only the default fields and selected DIMENSION fields are included by default. Specifying default fields returns an error.
            | **type** (string): [optional] Enum: The report type. [CAMPAIGN]
            | **startDate** (string): [required] Date in yyyy-MM-dd format. The report contains only metrics generated on the specified date range between startDate and endDate. The maximum date range between startDate and endDate is 31 days. The startDate can be up to 90 days older from today.
            | **dimensions** (list > string): [optional] List of dimensions to include in the report. Specify one or many comma-delimited strings of dimensions. For example: ["ORDER", "LINE_ITEM", "CREATIVE"]. Adding a dimension in this array determines the aggregation level of the report data and also adds the fields for that dimension in the report. If the list is null or empty, the aggregation of the report data is at ORDER level. The allowed values can be used together in this array as an allowed value in which case the report aggregation will be at the lowest aggregation level and the report will contain the fields for all the dimensions included in the report.
            | **timeUnit** (string): [optional] Enum: Adding timeUnit determines the aggregation level (SUMMARY or DAILY) of the report data. If the timeUnit is null or empty, the aggregation of the report data is at the SUMMARY level and aggregated at the time period specified. DAILY timeUnit is not supported for AUDIENCE report type. The report will contain the fields based on timeUnit. [SUMMARY]

        Returns:
            ApiResponse
        """
        path = fill_query_params(kwargs.pop('path'), dspAccountId)
        return self._request(path, data=kwargs.pop('body'), headers={'Accept': accept}, params=kwargs)

    @sp_endpoint('/accounts/{}/dsp/reports/{}', method='GET')
    def get_report(self, dspAccountId, reportId, accept='application/vnd.dspgetreports.v3+json', **kwargs) -> ApiResponse:
        r"""
        Gets a previously requested report specified by identifier.

        Keyword Args
            | path **dspAccountId** (string): Account Identifier for DSP. Please input DSP entity ID if you want to retrieve reports for a group of advertisers, or input DSP advertiser ID if you want to retrieve reports for a single advertiser. [required]
            | path **reportId** (string): The identifier of the requested report. [required]

        Returns:
            ApiResponse
        """
        path = fill_query_params(kwargs.pop('path'), dspAccountId, reportId)
        return self._request(path, headers={'Accept': accept}, params=kwargs)

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
