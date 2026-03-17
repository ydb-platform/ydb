from typing import Optional

from office365.entity import Entity


class AuthenticationFlowsPolicy(Entity):
    """
    Represents the policy configuration of self-service sign-up experience at a tenant level that lets external
    users request to sign up for approval. It contains information, such as the identifier, display name, and
    description, and indicates whether self-service sign-up is enabled for the policy.
    """

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The human-readable name of the policy."""
        return self.properties.get("displayName", None)
