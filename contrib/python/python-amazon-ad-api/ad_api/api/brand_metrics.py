from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class BrandMetrics(Client):
    """
    Brand Metrics provides a new measurement solution that quantifies opportunities for your brand at each stage of the customer journey on Amazon, and helps brands understand the value of different shopping engagements that impact stages of that journey. You can now access Awareness and Consideration indices that compare your performance to peers using models predictive of consideration and sales. Brand Metrics quantifies the number of customers in the awareness and consideration marketing funnel stages and is built at scale to measure all shopping engagements with your brand on Amazon, not just ad-attributed engagements. Additionally, BM breaks out key shopping engagements at each stage of the shopping journey, along with the Return on Engagement, so you can measure the historical sales following a consideration event or purchase.
    """

    @sp_endpoint('/insights/brandMetrics/report', method='POST')
    def post_report(self, **kwargs) -> ApiResponse:
        r"""Requests a Sponsored Brands report.

        Generates the Brand Metrics report in CSV or JSON format. Customize the report by passing a specific categoryNodeTreeName, categoryNodePath, brandName, reportStartDate, reportEndDate, lookbackPeriod, format or a list of metrics from the available metrics in the metrics field. If an empty request body is passed, report for the latest available report date in JSON format will get generated with all the available brands and metrics for an advertiser. The report may or may not contain the Brand Metrics data for one or more brands depending on data availability.

        Request body
            | **categoryNodeTreeName** (string): [optional] The node at the top of a browse tree. It is the start node of a tree. example: us-home
            | **categoryNodePath** (string): [optional] The hierarchical path that leads to a node starting with the root node. If no Category Node Name is passed, then all data available for all brands belonging to the entity are retrieved. example: List [ "Categories", "Snack Food", "Granola & Snack Bars" ]]
            | **brandName** (string): [optional] Brand Name. If no Brand Name is passed, then all data available for all brands belonging to the entity are retrieved. example: Mattress Mogul
            | **reportStartDate** (string($date)): [optional] Retrieves metrics with metricsComputationDate between reportStartDate and reportEndDate (inclusive). The date will be in the Coordinated Universal Time (UTC) timezone in YYYY-MM-DD format. If no date is passed in reportStartDate, all available metrics with metricsComputationDate till the reportEndDate will be provided. If no date is passed for either reportStartDate or reportEndDate, the metrics with the most receont metricsComputationDate will be returned. example: 2021-01-01
            | **lookBackPeriod** (string) [optional] Currently supported values: "1w" (one week), "1m" (one month) and "1cm" (one calendar month). This defines the period of time used to determine the number of shoppers in the metrics computation [ 1m, 1w, 1cm ]. default: 1w
            | **format** (string) [optional] Format of the report. [ JSON, CSV ]. default: JSON
            | **metrics** (list) [optional] Specify an array of string of metrics field names to include in the report. If no metric field names are specified, all metrics are returned.
            | **reportEndDate** (string($date)) [optional] Retrieves metrics with metricsComputationDate between reportStartDate and reportEndDate (inclusive). The date will be in the Coordinated Universal Time (UTC) timezone in YYYY-MM-DD format. If no date is passed in reportEndDate, all available metrics with metricsComputationDate from the reportStartDate will be provided. If no date is passed for either reportStartDate or reportEndDate, the metrics with the most receont metricsComputationDate will be returned. example: 2021-01-30

        Returns:
            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path')), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/insights/brandMetrics/report/{}', method='GET')
    def get_report(self, reportId, **kwargs) -> ApiResponse:
        r"""Gets a previously requested report specified by identifier.

        Keyword Args
            | path **reportId** (string): [required] The report Id to be fetched.

        Returns:
            ApiResponse
        """
        return self._request(fill_query_params(kwargs.pop('path'), reportId), params=kwargs)

    def download_report(self, **kwargs) -> ApiResponse:
        r"""Downloads the report previously get report specified by location
        (this is not part of the official Amazon Advertising API, is a helper
        method to download the report).

        In this API the url is self-authorized so to avoid double authorization
        we pass a simple header to ovrewrite the API auth headers that will lead
        to an error.

        kwarg parameter **file** if not provided will take the default amazon
        name from path download (add a path with slash / if you want a specific
        folder, do not add extension as the return will provide the right
        extension based on format choosed if needed)

        kwarg parameter **format** if not provided a format will return a url
        to download the report (this url has a expiration time)

        Keyword Args
            | **url** (string): [required] The location obatined from get_report.
            | **file** (string): [optional] The path to save the file if mode is download `json`, `zip` or `gzip`.
            | **format** (string): [optional] The mode to download the report: `raw`, `url`, `json`, `csv`,  Default (`url`)

        Returns:
            ApiResponse
        """
        headers = {'Accept': '*/*'}
        return self._download(self, params=kwargs, headers=headers)
