from typing import Optional

from office365.entity import Entity


class AgreementAcceptance(Entity):
    """
    Represents the current status of a user's response to a company's customizable terms of use agreement powered by
    Azure Active Directory (Azure AD).
    """

    @property
    def user_principal_name(self):
        # type: () -> Optional[str]
        """UPN of the user when the acceptance was recorded."""
        return self.properties.get("userPrincipalName", None)
