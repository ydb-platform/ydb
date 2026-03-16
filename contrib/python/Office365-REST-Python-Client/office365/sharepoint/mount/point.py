from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.mount.folder_info import MountedFolderInfo


class MountPoint(Entity):
    """"""

    @staticmethod
    def get_mounted_folder_info(
        context, target_site_id, target_web_id, target_unique_id
    ):
        """
        :type context: office365.sharepoint.client_context.ClientContext
        :param str target_site_id:
        :param str target_web_id:
        :param str target_unique_id:
        """
        return_type = MountedFolderInfo(context)
        payload = {
            "targetSiteId": target_site_id,
            "targetWebId": target_web_id,
            "targetUniqueId": target_unique_id,
        }
        qry = ServiceOperationQuery(
            context.web, "GetMountedFolderInfo", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type
