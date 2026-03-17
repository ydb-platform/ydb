from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Reports(Client):
    """Sponsored Brand Reports

    Documentation: https://advertising.amazon.com/API/docs/en-us/reference/sponsored-brands/2/reports

    """

    @sp_endpoint('/v2/hsa/{}/report', method='POST')
    def post_report(self, recordType, **kwargs) -> ApiResponse:
        r"""Requests a Sponsored Brands report.

        Request the creation of a performance report for all entities of a single type which have performance data to report. Record types can be: `campaigns`, `adGroups`, and `keywords`.

        Keyword Args
            | path **recordType** (string): The type of entity for which the report should be generated. Supported record types: `campaigns`, `adGroups`, and `keywords`

        Request body
            | **segment** (string): [required] Dimension on which to segment the report. Available values : `placement`, `query`
            | **reportDate** (string): [required] The date for which to retrieve the performance report in `YYYYMMDD` format. The time zone is specified by the profile used to request the report. If this date is today, then the performance report may contain partial information. Reports are not available for data older than 60 days.
            | **metrics** (string) [optional] A comma-separated list of the metrics to be included in the report.
            | **creativeType** (string) [optional] Set to `video` to request a Sponsored Brands video report.

        Returns:
            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), recordType), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/v2/reports/{}', method='GET')
    def get_report(self, reportId, **kwargs) -> ApiResponse:
        r"""Gets a previously requested report specified by identifier.

        Keyword Args
            | path **reportId** (string): [required] The report identifier.

        Request body
            | **creativeType** (string): [optional] Set to `video` to retrieve a Sponsored Brands video report.

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), reportId), params=kwargs)

    def download_report(self, **kwargs) -> ApiResponse:
        r"""Downloads the report previously get report specified by location
        (this is not part of the official Amazon Advertising API, is a helper
        method to download the report). Take in mind that a direct download of
        location returned in get_report will return 401 - Unauthorized.

        kwarg parameter **file** if not provided will take the default amazon
        name from path download (add a path with slash / if you want a specific
        folder, do not add extension as the return will provide the right
        extension based on format choosed if needed)

        kwarg parameter **format** if not provided a format will return a url
        to download the report (this url has a expiration time)

        Keyword Args
            | **url** (string): [required] The location obatined from get_report.
            | **file** (string): [optional] The path to save the file if mode is download `json`, `zip` or `gzip`.
            | **format** (string): [optional] The mode to download the report: `data` (list), `raw`, `url`, `json`, `zip`, `gzip`. Default (`url`)

        Returns:
            ApiResponse
        """
        return self._download(self, params=kwargs)
