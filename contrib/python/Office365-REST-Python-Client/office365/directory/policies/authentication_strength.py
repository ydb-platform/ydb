from office365.directory.authentication.strength_usage import (
    AuthenticationStrengthUsage,
)
from office365.directory.policies.update_allowed_combinations_result import (
    UpdateAllowedCombinationsResult,
)
from office365.entity import Entity
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery


class AuthenticationStrengthPolicy(Entity):
    """
    A collection of settings that define specific combinations of authentication methods and metadata.
    The authentication strength policy, when applied to a given scenario using Azure AD Conditional Access,
    defines which authentication methods must be used to authenticate in that scenario. An authentication strength
    may be built-in or custom (defined by the tenant) and may or may not fulfill the requirements to grant an MFA claim.
    """

    def usage(self):
        """
        Allows the caller to see which Conditional Access policies reference a specified authentication strength policy.
        The policies are returned in two collections, one containing Conditional Access policies that require an
        MFA claim and the other containing Conditional Access policies that do not require such a claim.
        Policies in the former category are restricted in what kinds of changes may be made to them to prevent
        undermining the MFA requirement of those policies.
        """
        return_type = ClientResult(self.context, AuthenticationStrengthUsage())
        qry = FunctionQuery(self, "usage", None, return_type)
        self.context.add_query(qry)
        return return_type

    def update_allowed_combinations(self, allowed_combinations=None):
        """
        Update the allowedCombinations property of an authenticationStrengthPolicy object.
        To update other properties of an authenticationStrengthPolicy object,
        use the Update authenticationStrengthPolicy method.

        :param list[str] allowed_combinations: The authentication method combinations allowed by this authentication
             strength policy.
        """
        return_type = ClientResult(self.context, UpdateAllowedCombinationsResult())
        payload = {"allowedCombinations": allowed_combinations}
        qry = ServiceOperationQuery(
            self, "updateAllowedCombinations", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
