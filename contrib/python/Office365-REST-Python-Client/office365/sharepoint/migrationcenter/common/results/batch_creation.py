from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class BatchCreationResult(ClientValue):
    """"""

    def __init__(
        self,
        created_count=None,
        created_task_id_list=None,
        error_code=None,
        error_message=None,
        field_error=None,
        processing_milliseconds=None,
        total_count=None,
    ):
        self.CreatedCount = created_count
        self.CreatedTaskIdList = StringCollection(created_task_id_list)
        self.ErrorCode = error_code
        self.ErrorMessage = error_message
        self.FieldError = field_error
        self.ProcessingMilliseconds = processing_milliseconds
        self.TotalCount = total_count

    @property
    def entity_type_name(self):
        # type: () -> str
        return "Microsoft.Online.SharePoint.MigrationCenter.Common.BatchCreationResult"
