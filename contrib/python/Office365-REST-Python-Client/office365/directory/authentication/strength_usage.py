from office365.directory.policies.conditional_access import ConditionalAccessPolicy
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class AuthenticationStrengthUsage(ClientValue):
    """
    An object containing two collections of Conditional Access policies that reference the specified authentication
    strength. One collection references Conditional Access policies that require an MFA claim; the other collection
    references Conditional Access policies that don't require such a claim.
    """

    def __init__(self, mfa=None, none=None):
        """
        :param list[ConditionalAccessPolicy] mfa: A collection of Conditional Access policies that reference the
             specified authentication strength policy and that require an MFA claim.
        :param list[ConditionalAccessPolicy] none: A collection of Conditional Access policies that reference the
             specified authentication strength policy and that do not require an MFA claim.
        """
        self.mfa = ClientValueCollection(ConditionalAccessPolicy, mfa)
        self.none = ClientValueCollection(ConditionalAccessPolicy, none)
