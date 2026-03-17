from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.search.promoted_results_operations_result import (
    PromotedResultsOperationsResult,
)
from office365.sharepoint.search.query.configuration import QueryConfiguration
from office365.sharepoint.search.reports.base import ReportBase


class SearchSetting(Entity):
    """This object provides the REST operations defined under search settings."""

    def __init__(self, context):
        super(SearchSetting, self).__init__(
            context, ResourcePath("Microsoft.Office.Server.Search.REST.SearchSetting")
        )

    def get_query_configuration(
        self,
        call_local_search_farms_only=True,
        skip_group_object_id_lookup=None,
        throw_on_remote_api_check=None,
    ):
        """
        This operation gets the query configuration from the server. This operation requires that the Search Service
        Application is partitioned. If the Search Service Application is not partitioned the operations returns
        HTTP code 400, not authorized.

        :param bool call_local_search_farms_only: This is a flag that indicates to only call the local search farm.
        :param bool skip_group_object_id_lookup:
        :param bool throw_on_remote_api_check:
        """
        return_type = ClientResult(self.context, QueryConfiguration())
        payload = {
            "callLocalSearchFarmsOnly": call_local_search_farms_only,
            "skipGroupObjectIdLookup": skip_group_object_id_lookup,
            "throwOnRemoteApiCheck": throw_on_remote_api_check,
        }
        qry = ServiceOperationQuery(
            self, "getqueryconfiguration", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def export_search_reports(
        self,
        tenant_id,
        report_type=None,
        interval=None,
        start_date=None,
        end_date=None,
        site_collection_id=None,
    ):
        """
        :param str tenant_id:
        :param str report_type:
        :param str interval:
        :param str start_date:
        :param str end_date:
        :param str site_collection_id:
        """
        return_type = ClientResult(self.context, ReportBase())
        payload = {
            "TenantId": tenant_id,
            "ReportType": report_type,
            "Interval": interval,
            "StartDate": start_date,
            "EndDate": end_date,
            "SiteCollectionId": site_collection_id,
        }
        qry = ServiceOperationQuery(
            self, "ExportSearchReports", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ping_admin_endpoint(self):
        """ """
        return_type = ClientResult[bool](self.context)
        qry = ServiceOperationQuery(
            self, "PingAdminEndpoint", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_promoted_result_query_rules(
        self, site_collection_level=None, offset=None, number_of_rules=None
    ):
        """
        The operation is called to retrieve the promoted results (also called Best Bets) for a tenant or a
        site collection.

        :param bool site_collection_level: This parameter is used by the protocol server to decide which promoted
           results to return to the client. If the parameter is true, the promoted results for the current
           site collection are returned. If the parameter is false, all promoted results for the
           tenant/Search Service Application are returned.
        :param int offset: This parameter is the offset into the collection of promoted results. Default value is zero.
           It is used to page through a large result set.
        :param int number_of_rules: his parameter is the number of promoted results that are returned in the operation.
            Default value is 100. It is used together with the offset to page through a large result set.
        """
        return_type = ClientResult(self.context, PromotedResultsOperationsResult())
        payload = {
            "siteCollectionLevel": site_collection_level,
            "offset": offset,
            "numberOfRules": number_of_rules,
        }
        qry = ServiceOperationQuery(
            self, "getpromotedresultqueryrules", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SearchSetting"
