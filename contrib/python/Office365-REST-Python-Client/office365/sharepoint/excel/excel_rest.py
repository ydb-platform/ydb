from office365.runtime.client_runtime_context import ClientRuntimeContext


class ExcelService(ClientRuntimeContext):
    """
    A REST API for Excel Services enables operations against Excel workbooks by using operations specified in the
    HTTP standard

    Note:
    The Excel Services REST API for SharePoint Online will no longer be supported for Microsoft 365 accounts
    from February 28th, 2022 forward. Instead, please use the REST API that’s part of the Microsoft Graph endpoint.
    """

    def __init__(self, context):
        """
        Excel Services REST API client
        https://docs.microsoft.com/en-us/sharepoint/dev/general-development/excel-services-rest-api
        """
        super(ExcelService, self).__init__()

    def authenticate_request(self, request):
        pass

    def service_root_url(self):
        return "{0}/_vti_bin/ExcelRest.aspx"

    def pending_request(self):
        pass

    def get_workbook(self, list_name, file_name):
        raise NotImplementedError("get_workbook")
