from office365.runtime.client_result import ClientResult
from office365.runtime.odata.v4.upload_session import UploadSession
from office365.runtime.queries.service_operation import ServiceOperationQuery


class UploadSessionQuery(ServiceOperationQuery):
    """
    The UploadSession query provides information about how to upload large files to OneDrive, OneDrive for
    Business, or SharePoint document libraries
    """

    def __init__(self, binding_type, parameters_type):
        super(UploadSessionQuery, self).__init__(
            binding_type, "createUploadSession", None, parameters_type
        )

    @property
    def upload_session_url(self):
        return self.return_type.value.uploadUrl

    @property
    def return_type(self):
        # type: () -> ClientResult[UploadSession]
        if self._return_type is None:
            self._return_type = ClientResult(self.context, UploadSession())
        return self._return_type
