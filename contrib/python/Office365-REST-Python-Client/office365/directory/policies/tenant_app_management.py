from office365.directory.policies.base import PolicyBase


class TenantAppManagementPolicy(PolicyBase):
    """
    Tenant-wide application authentication method policy to enforce app management restrictions for all applications
    and service principals. This policy applies to all apps and service principals unless overridden when an
    appManagementPolicy is applied to the object.
    """
