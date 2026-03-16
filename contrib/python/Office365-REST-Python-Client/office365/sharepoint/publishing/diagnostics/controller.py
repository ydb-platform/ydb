from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.publishing.diagnostics.page_diagnostics import PageDiagnostics


class PageDiagnosticsController(Entity):
    def __init__(self, context):
        static_path = ResourcePath("SP.Publishing.PageDiagnosticsController")
        super(PageDiagnosticsController, self).__init__(context, static_path)

    def by_page(self, page_relative_file_path):
        """
        :param str page_relative_file_path:
        """
        return_type = ClientResult(self.context, PageDiagnostics())
        payload = {"pageRelativeFilePath": page_relative_file_path}
        qry = ServiceOperationQuery(self, "ByPage", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Publishing.PageDiagnosticsController"
