from office365.directory.protection.policy.base import ProtectionPolicyBase


class OneDriveForBusinessProtectionPolicy(ProtectionPolicyBase):
    """
    Contains details about protection policies applied to Microsoft 365 data in an organization.
    Protection policies are defined by the Global Admin (or the SharePoint Online Admin or Exchange Online Admin)
    and include what data to protect, when to protect it, and for what time period to retain the protected data
    for a single Microsoft 365 service.
    """
