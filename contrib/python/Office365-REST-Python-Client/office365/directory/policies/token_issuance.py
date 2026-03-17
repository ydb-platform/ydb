from office365.directory.policies.sts import StsPolicy


class TokenIssuancePolicy(StsPolicy):
    """
    Represents the policy to specify the characteristics of SAML tokens issued by Azure AD. You can use token-issuance
    policies to:

     - Set signing options
     - Set signing algorithm
     - Set SAML token version
    """
