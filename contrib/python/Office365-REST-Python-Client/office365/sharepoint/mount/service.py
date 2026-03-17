from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.mount.requests.get_remote_item_Info import (
    GetRemoteItemInfoRequest,
)


class MountService(Entity):
    @staticmethod
    def get_remote_item_info(context, remote_item_unique_ids):
        """
        :param office365.sharepoint.client_context.ClientContext context: client context
        :param list[str] remote_item_unique_ids:
        """
        return_type = ClientResult(context, str())
        payload = {"request": GetRemoteItemInfoRequest(remote_item_unique_ids)}
        qry = ServiceOperationQuery(
            context.web, "GetRemoteItemInfo", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.AddToOneDrive.MountService"
