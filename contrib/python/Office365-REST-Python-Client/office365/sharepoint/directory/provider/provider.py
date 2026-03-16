from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.directory.provider.object_data import DirectoryObjectData
from office365.sharepoint.entity import Entity


class SharePointDirectoryProvider(Entity):
    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "SP.Directory.Provider.SharePointDirectoryProvider"
            )
        super(SharePointDirectoryProvider, self).__init__(context, resource_path)

    def check_site_availability(self, site_url):
        """"""
        from office365.sharepoint.directory.helper import SPHelper

        return SPHelper.check_site_availability(self.context, site_url)

    def read_directory_object(self, data):
        # type: (DirectoryObjectData) -> ClientResult[DirectoryObjectData]
        """"""
        return_type = ClientResult(self.context, DirectoryObjectData())
        payload = {"data": data}
        qry = ServiceOperationQuery(
            self, "ReadDirectoryObject", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Directory.Provider.SharePointDirectoryProvider"
