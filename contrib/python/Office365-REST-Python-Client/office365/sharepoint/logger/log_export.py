from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.logger.logFileInfoCollection import LogFileInfoCollection


class LogExport(Entity):
    def __init__(self, context):
        """This is the primary class that should be instantiated to obtain metadata about the
        logs that you can download."""
        super(LogExport, self).__init__(
            context, ResourcePath("Microsoft.Online.SharePoint.SPLogger.LogExport")
        )

    def get_files(self):
        """ """
        return_type = LogFileInfoCollection(self.context)
        qry = ServiceOperationQuery(self, "GetFiles", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_log_types(self):
        """ """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(self, "GetLogTypes", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.SPLogger.LogExport"
