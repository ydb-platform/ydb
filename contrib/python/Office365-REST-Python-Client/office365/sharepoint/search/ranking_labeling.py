from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class RankingLabeling(Entity):
    """Provides methods for getting and adding relevance judgments"""

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.SharePoint.Client.Search.Query.RankingLabeling"
        )
        super(RankingLabeling, self).__init__(context, static_path)

    def add_judgment(self, user_query, url, label_id):
        """
        Adds a single relevance judgment for the specified query and URL pair.

        :param str user_query: User query for which the relevance judgment is added.
        :param str url: URL for which the relevance judgment is added.
        :param str label_id: The judgment for this query-URL pair as represented by an Int16. This value MUST
            be between 1 and 5 inclusive.
        """
        payload = {"userQuery": user_query, "url": url, "labelId": label_id}
        qry = ServiceOperationQuery(self, "AddJudgment", None, payload)
        self.context.add_query(qry)
        return self

    def normalize_result_url(self, url):
        # type: (str) -> ClientResult[str]
        """
        A URL string after normalization. The input and output URL strings MUST resolve to the same document.

        :param str url: The URL for which the relevance judgment is added.
        """
        return_type = ClientResult(self.context)
        payload = {"url": url}
        qry = ServiceOperationQuery(
            self, "NormalizeResultUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.RankingLabeling"
