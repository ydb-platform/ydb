from office365.entity import Entity


class MultiTenantOrganization(Entity):
    """
    Defines an organization with more than one instance of Microsoft Entra ID. A multitenant organization enables
    multiple tenants to collaborate like a single entity.

    There can only be one multitenant organization per active tenant. It is not possible to be part of multiple
    multitenant organizations.
    """
