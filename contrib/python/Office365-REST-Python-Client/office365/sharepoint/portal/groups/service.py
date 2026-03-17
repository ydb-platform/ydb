import random
from typing import AnyStr

from office365.runtime.client_result import ClientResult
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class GroupService(Entity):
    def get_group_image(
        self, group_id, image_hash=None, image_color=None, return_type=None
    ):
        # type: (str, str, str, ClientResult) -> ClientResult[AnyStr]
        if return_type is None:
            return_type = ClientResult(self.context)
        if image_hash is None:
            image_hash = random.getrandbits(64)
        qry = ServiceOperationQuery(
            self, "GetGroupImage", None, None, None, return_type
        )

        def _create_request(request):
            # type: (RequestOptions) -> None
            request.url += "?id='{0}'&hash={1}".format(group_id, image_hash)
            request.method = HttpMethod.Get

        self.context.add_query(qry).before_query_execute(_create_request)
        return return_type

    def sync_group_properties(self):
        """ """
        qry = ServiceOperationQuery(self, "SyncGroupProperties")
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.GroupService"
