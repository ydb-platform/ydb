from office365.runtime.client_value import ClientValue


class AuditResource(ClientValue):
    """A class containing the properties for Audit Resource."""

    def __init__(self, audit_resource_type=None):
        """
        :param str audit_resource_type: Audit resource's type.
        """
        self.auditResourceType = audit_resource_type
