from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.search.simple_data_table import SimpleDataTable


class DocumentCrawlLog(Entity):
    """This object contains methods that can be used by the protocol client to retrieve information
    about items that were crawled."""

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.SharePoint.Client.Search.Administration.DocumentCrawlLog"
        )
        super(DocumentCrawlLog, self).__init__(context, static_path)

    def get_crawled_urls(
        self,
        get_count_only=False,
        max_rows=None,
        query_string=None,
        content_source_id=None,
    ):
        """
        Retrieves information about all the contents that were crawled.

        :param bool get_count_only: f true, only the count of the contents crawled MUST be returned.
             If false, all the information about the crawled contents MUST be returned.
        :param int max_rows:
        :param str query_string:
        :param int content_source_id:
        """
        return_type = ClientResult(self.context, SimpleDataTable())
        payload = {
            "getCountOnly": get_count_only,
            "maxRows": max_rows,
            "queryString": query_string,
            "contentSourceID": content_source_id,
        }
        qry = ServiceOperationQuery(
            self, "GetCrawledUrls", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_unsuccesful_crawled_urls(self, display_url=None):
        """
        Retrieves information about the contents that failed crawling.

        :param str display_url:
        """
        return_type = ClientResult(self.context, SimpleDataTable())
        payload = {"displayUrl": display_url}
        qry = ServiceOperationQuery(
            self, "GetUnsuccesfulCrawledUrls", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Administration.DocumentCrawlLog"
