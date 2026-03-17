from office365.entity import Entity


class AdminConsentRequestPolicy(Entity):
    """
    Represents the policy for enabling or disabling the Microsoft Entra admin consent workflow.
    The admin consent workflow allows users to request access for apps that they wish to use and that require admin
    authorization before users can use the apps to access organizational data.
    There is a single adminConsentRequestPolicy per tenant.
    """
