"""Helpers to create Azure Data Explorer specific Grafana queries."""

import attr

TIME_SERIES_RESULT_FORMAT = 'time_series'
TABLE_RESULT_FORMAT = 'table'
ADX_TIME_SERIES_RESULT_FORMAT = 'time_series_adx_series'


@attr.s
class AzureDataExplorerTarget(object):
    """
    Generates Azure Data Explorer target JSON structure.

    Link to Azure Data Explorer datasource Grafana plugin:
    https://grafana.com/grafana/plugins/grafana-azure-data-explorer-datasource/

    Azure Data Explorer docs on query language (KQL):
    https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/

    :param database: Database to execute query on
    :param query: Query in Kusto Query Language (KQL)
    :param resultFormat: Output format of the query result
    :param alias: legend alias
    :param refId: target reference id
    """

    database = attr.ib(default="")
    query = attr.ib(default="")
    resultFormat = attr.ib(default=TIME_SERIES_RESULT_FORMAT)
    alias = attr.ib(default="")
    refId = attr.ib(default="")

    def to_json_data(self):
        return {
            'database': self.database,
            'query': self.query,
            'resultFormat': self.resultFormat,
            'alias': self.alias,
            'refId': self.refId
        }
