from office365.entity import Entity


class CompanySubscription(Entity):
    """Represents a commercial subscription for a tenant. Use the values of skuId and serviceStatus > servicePlanId
    to assign licenses to unassigned users and groups through the user: assignLicense and group: assignLicense
    APIs respectively."""
