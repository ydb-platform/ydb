from office365.entity import Entity


class RiskDetection(Entity):
    """
    Represents information about a detected risk in an Azure AD tenant.

    Azure AD continually evaluates user risks and app or user sign-in risks based on various signals
    and machine learning.
    This API provides programmatic access to all risk detections in your Azure AD environment.
    """
