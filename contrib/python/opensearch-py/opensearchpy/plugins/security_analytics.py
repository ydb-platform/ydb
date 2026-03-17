# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from ..client.utils import NamespacedClient, query_params


class SecurityAnalyticsClient(NamespacedClient):
    @query_params(
        "alertState",
        "detectorType",
        "detector_id",
        "endTime",
        "error_trace",
        "filter_path",
        "human",
        "missing",
        "pretty",
        "searchString",
        "severityLevel",
        "size",
        "sortOrder",
        "sortString",
        "source",
        "startIndex",
        "startTime",
    )
    def get_alerts(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieve alerts related to a specific detector type or detector ID.


        :arg alertState: Used to filter by alert state. Optional. Valid
            choices are ACKNOWLEDGED, ACTIVE, COMPLETED, DELETED, ERROR.
        :arg detectorType: The type of detector used to fetch alerts.
            Optional when `detector_id` is specified. Otherwise required.
        :arg detector_id: The ID of the detector used to fetch alerts.
            Optional when `detectorType` is specified. Otherwise required.
        :arg endTime: The end timestamp (in ms) of the time window in
            which you want to retrieve alerts. Optional.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg missing: Used to sort by whether the field `missing` exists
            or not in the documents associated with the alert. Optional.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg searchString: The alert attribute you want returned in the
            search. Optional.
        :arg severityLevel: Used to filter by alert severity level.
            Optional. Valid choices are 1, 2, 3, 4, 5, ALL.
        :arg size: The maximum number of results returned in the
            response. Optional. Default is 20.
        :arg sortOrder: The order used to sort the list of findings.
            Possible values are `asc` or `desc`. Optional. Valid choices are asc,
            desc.
        :arg sortString: The string used by Security Analytics to sort
            the alerts. Optional. Default is start_time.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg startIndex: The pagination index. Optional. Default is 0.
        :arg startTime: The beginning timestamp (in ms) of the time
            window in which you want to retrieve alerts. Optional.
        """
        return self.transport.perform_request(
            "GET",
            "/_plugins/_security_analytics/alerts",
            params=params,
            headers=headers,
        )

    @query_params(
        "detectionType",
        "detectorType",
        "detector_id",
        "endTime",
        "error_trace",
        "filter_path",
        "findingIds",
        "human",
        "missing",
        "pretty",
        "searchString",
        "severity",
        "size",
        "sortOrder",
        "sortString",
        "source",
        "startIndex",
        "startTime",
    )
    def get_findings(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieve findings related to a specific detector type or detector ID.


        :arg detectionType: The detection type that dictates the
            retrieval type for the findings. When the detection type is `threat`, it
            fetches threat intelligence feeds. When the detection type is `rule`,
            findings are fetched based on the detector’s rule. Optional. Valid
            choices are rule, threat.
        :arg detectorType: The type of detector used to fetch alerts.
            Optional when the `detector_id` is specified. Otherwise required.
        :arg detector_id: The ID of the detector used to fetch alerts.
            Optional when the `detectorType` is specified. Otherwise required.
        :arg endTime: The end timestamp (in ms) of the time window in
            which you want to retrieve findings. Optional.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg findingIds: The comma-separated id list of findings for
            which you want retrieve details. Optional.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg missing: Used to sort by whether the field `missing` exists
            or not in the documents associated with the finding. Optional.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg searchString: The finding attribute you want returned in
            the search. To search in a specific index, specify the index name in the
            request path. For example, to search findings in the indexABC index, use
            `searchString=indexABC’. Optional.
        :arg severity: The rule severity for which retrieve findings.
            Severity can be `critical`, `high`, `medium`, or `low`. Optional. Valid
            choices are critical, high, low, medium.
        :arg size: The maximum number of results returned in the
            response. Optional. Default is 20.
        :arg sortOrder: The order used to sort the list of findings.
            Possible values are `asc` or `desc`. Optional. Valid choices are asc,
            desc.
        :arg sortString: The string used by the Alerting plugin to sort
            the findings. Optional. Default is timestamp.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg startIndex: The pagination index. Optional. Default is 0.
        :arg startTime: The beginning timestamp (in ms) of the time
            window in which you want to retrieve findings. Optional.
        """
        return self.transport.perform_request(
            "GET",
            "/_plugins/_security_analytics/findings/_search",
            params=params,
            headers=headers,
        )

    @query_params(
        "detector_type",
        "error_trace",
        "filter_path",
        "finding",
        "human",
        "nearby_findings",
        "pretty",
        "source",
        "time_window",
    )
    def search_finding_correlations(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        List correlations for a finding.


        :arg detector_type: The log type of findings you want to
            correlate with the specified finding. Required.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg finding: The finding ID for which you want to find other
            findings that are correlated. Required.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg nearby_findings: The number of nearby findings you want to
            return. Optional. Default is 10.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg time_window: The time window (in ms) in which all of the
            correlations must have occurred together. Optional. Default is 300000.
        """
        return self.transport.perform_request(
            "GET",
            "/_plugins/_security_analytics/findings/correlate",
            params=params,
            headers=headers,
        )
