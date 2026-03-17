from office365.entity_collection import EntityCollection
from office365.intune.audit.event import AuditEvent
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.types.collections import StringCollection


class AuditEventCollection(EntityCollection[AuditEvent]):
    def __init__(self, context, resource_path=None):
        super(AuditEventCollection, self).__init__(context, AuditEvent, resource_path)

    def get_audit_categories(self):
        """Not yet documented"""
        return_type = ClientResult(self.context, StringCollection())
        qry = FunctionQuery(self, "getAuditCategories", None, return_type)
        self.context.add_query(qry)
        return return_type

    def get_audit_activity_types(self, category):
        """Not yet documented"""
        return_type = ClientResult(self.context, StringCollection())
        params = {"category": category}
        qry = FunctionQuery(self, "getAuditActivityTypes", params, return_type)
        self.context.add_query(qry)
        return return_type
