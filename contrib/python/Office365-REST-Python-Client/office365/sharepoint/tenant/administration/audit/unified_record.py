from office365.runtime.client_value import ClientValue
from office365.sharepoint.tenant.administration.audit.data import AuditData


class UnifiedAuditRecord(ClientValue):

    def __init__(
        self,
        audit_data=AuditData(),
        creation_date=None,
        operation=None,
        record_id=None,
        record_type=None,
        user_id=None,
    ):
        self.AuditData = audit_data
        self.CreationDate = creation_date
        self.Operation = operation
        self.RecordId = record_id
        self.RecordType = record_type
        self.UserId = user_id

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.TenantAdmin.UnifiedAuditRecord"
