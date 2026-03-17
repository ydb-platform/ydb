from typing import TYPE_CHECKING

from office365.reports.report import Report
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery

if TYPE_CHECKING:
    from office365.reports.root import ReportRoot


def create_report_query(report_root, report_name, period=None, return_stream=False):
    """
    Construct Report query

    :param ReportRoot report_root: Report container
    :param str report_name: Report name
    :param str period: Specifies the length of time over which the report is aggregated.
        The supported values for {period_value} are: D7, D30, D90, and D180. These values follow the format
        Dn where n represents the number of days over which the report is aggregated. Required.
    :param bool return_stream: If true, return a stream of report data.
    """
    params = {
        "period": period,
    }
    if return_stream:
        return_type = ClientResult(report_root.context, bytes())
    else:
        return_type = ClientResult(report_root.context, Report())
    return FunctionQuery(report_root, report_name, params, return_type)
